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
			const payload = {
				output_format: "polling",
				current_bookmark: bookmark 
			};

			const res = await fetch(url, {
				method,
				headers,
				body: JSON.stringify(payload),
			});
			const { result } = await res.json();

			if (!result?.signed_url) throw new Error("Missing `signed_url`");

			const dumpResponse = await fetch(result.signed_url);
			if (!dumpResponse.ok) throw new Error("Failed to fetch dump file");

			// Use current datetime in filename
			const now = new Date();
			const iso = now.toISOString().replace(/[:.]/g, "-");
			const filename = `boar-db-bkp-${iso}`;

			// Store backup in R2
			await this.env.BoarParkDBBkp.put(filename, dumpResponse.body);

			// Prune old backups (keep only last 7 days)
			const listResp = await this.env.BoarParkDBBkp.list();

			for (const obj of listResp.objects) {
				const created = new Date(obj.uploaded || obj.uploadedAt || obj.uploaded_on || obj.uploadedOn); // fallback safety
				if (isNaN(created)) continue;

				const ageInDays = (now - created) / (1000 * 60 * 60 * 24);
				if (ageInDays > 7) {
					await this.env.BoarParkDBBkp.delete(obj.key);
				}
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
