# Production startup

Railway must use `gunicorn "craft_v2:create_app_v2()" --bind 0.0.0.0:$PORT --timeout 120 --workers 1`. Update its service override as part of deployment; the Procfile now has the same command. The previous `craft_v2:app` target no longer exists.

Importing craft_v2 is inert. The factory opens the analytics volume, initializes the V2 repository and constructs routes. Scheduler startup remains controlled by CRAFT_AUTO_SYNC. Keep CRAFT_AUTO_SYNC=0 during cutover and verification.

One worker remains mandatory while the existing schedulers and sync admission locks are process-local. Multiple workers would duplicate schedules and defeat admission control. Do not increase the worker count until scheduling and admission control move to a single external owner. A framework swap around sending is specifically excluded: ambiguous provider outcomes still require reconciliation.

Before deployment, inspect /api/sync-status and require no running or interrupted traversal. Preserve the mounted analytics database. Compare authenticated row-count diagnostics before and after deployment. External sending stays disabled.
