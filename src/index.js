import { WorkflowEntrypoint } from "cloudflare:workers";

export class backupWorkflow extends WorkflowEntrypoint {
	async run(event, step) {
		const { accountId, databaseId } = event.payload;

		const url = `https://api.cloudflare.com/client/v4/accounts/${accountId}/d1/database/${databaseId}/export`;
		const method = "POST";
		const headers = new Headers();
		headers.append("Content-Type", "application/json");
		headers.append("Authorization", `Bearer ${this.env.D1_REST_API_TOKEN}`);

		const bookmark = await step.do(
			`Starting backup for ${databaseId}`,
			async () => {
				const payload = { output_format: "polling" };

				const res = await fetch(url, {
					method,
					headers,
					body: JSON.stringify(payload),
				});
				const { result } = await res.json();

				if (!result?.at_bookmark) throw new Error("Missing `at_bookmark`");

				return result.at_bookmark;
			}
		);

		await step.do("Check backup status, store on R2, and prune old backups", async () => {
		  let signedUrl = null;
		
		  // Poll until complete (or give up after N tries)
		  for (let i = 0; i < 60; i++) {
		    const res = await fetch(url, {
		      method,
		      headers,
		      body: JSON.stringify({
		        output_format: "polling",
		        current_bookmark: bookmark,
		      }),
		    });
		
		    if (!res.ok) {
		      const text = await res.text().catch(() => "");
		      throw new Error(`Export poll failed: ${res.status} ${text}`);
		    }
		
		    const json = await res.json();
		    const r = json?.result;
		
		    if (!r) throw new Error("Missing `result` in export response");
		
		    if (r.status === "complete") {
		      signedUrl = r?.result?.signed_url;
		      break;
		    }
		
		    // Still active; wait a bit before polling again
		    await step.sleep(`poll-wait-${i}`, "2 seconds");
		  }
		
		  if (!signedUrl) throw new Error("Export did not complete / missing `signed_url`");
		
		  const dumpResponse = await fetch(signedUrl);
		  if (!dumpResponse.ok) throw new Error(`Failed to fetch dump file: ${dumpResponse.status}`);
		
		  const now = new Date();
		  const iso = now.toISOString().replace(/[:.]/g, "-");
		  const filename = `boar-db-bkp-${iso}.sql`;
		
		  await this.env.BoarParkDBBkp.put(filename, dumpResponse.body);
		
		  // Prune old backups (keep only last 7 days)
		  const listResp = await this.env.BoarParkDBBkp.list();
		  for (const obj of listResp.objects) {
		    // R2 list objects expose `uploaded` (Date) in Workers runtime
		    const created = obj.uploaded instanceof Date ? obj.uploaded : new Date(obj.uploaded);
		    if (Number.isNaN(created.getTime())) continue;
		
		    const ageInDays = (now - created) / (1000 * 60 * 60 * 24);
		    if (ageInDays > 7) await this.env.BoarParkDBBkp.delete(obj.key);
		  }
		});
	}
}

export default {
	async fetch(req, env) {
		return new Response("Not found", { status: 404 });
	},

	async scheduled(controller, env, ctx) {
		const params = {
			accountId: env.ACCOUNT_ID,
			databaseId: env.DATABASE_ID,
		};

		const instance = await env.BACKUP_WORKFLOW.create({ params });
		console.log(`Started backup workflow with ID: ${instance.id}`);
	},
};
