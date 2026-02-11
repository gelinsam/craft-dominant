import os
import sys
import threading
import logging

log = logging.getLogger('craft')

def main():
    """Entry point for local development (not used by gunicorn/Railway)."""
    from craft_unified import CraftDominant

    c = CraftDominant()

    def do_sync():
        try:
            api_key = os.environ.get("EVENTBRITE_API_KEY")
            if not api_key:
                log.error("EVENTBRITE_API_KEY not set - skipping sync")
                return
            log.info("Starting background sync...")
            result = c.sync(api_key, 2)
            log.info(f"Sync complete: {result.get('events', 0)} events, "
                     f"{result.get('orders', 0)} orders, "
                     f"{result.get('customers', 0)} customers")
            if result.get('errors'):
                log.warning(f"Sync had {len(result['errors'])} errors")
        except Exception as e:
            log.error(f"Sync failed: {e}", exc_info=True)

    threading.Thread(target=do_sync, daemon=True).start()

    port = int(os.environ.get("PORT", "8080"))
    try:
        c.serve(host="0.0.0.0", port=port)
    except TypeError:
        c.serve()

if __name__ == "__main__":
    main()
