import os
import threading
from craft_unified import CraftDominant

def main():
    c = CraftDominant()

    def do_sync():
        # run once at boot
        c.sync(os.environ["EVENTBRITE_API_KEY"], 2)

    threading.Thread(target=do_sync, daemon=True).start()

    # IMPORTANT: Railway expects host 0.0.0.0 and PORT
    port = int(os.environ.get("PORT", "8080"))

    # If CraftDominant.serve accepts host/port, do this:
    try:
        c.serve(host="0.0.0.0", port=port)
    except TypeError:
        # If serve() doesn't accept args, then serve() must be fixed internally
        # to bind to 0.0.0.0 and PORT. For now just call it.
        c.serve()

if __name__ == "__main__":
    main()
