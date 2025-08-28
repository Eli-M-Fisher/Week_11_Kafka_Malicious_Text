from enricher import Enricher

def main():
    """
    entry point for the Enricher service.
    initializes and runs the service with basic error handling
    """
    print("Enricher service is starting...", flush=True)
    try:
        service = Enricher()
        service.run()
    except Exception as e:
        print(f"Error while running Enricher: {e}", flush=True)


if __name__ == "__main__":
    main()