from enricher import Enricher

def main():
    """
    the entry point for the Enricher service.
    """
    print("Enricher service is starting...")

    try:
        enricher = Enricher()
        enricher.run()
    except Exception as e:
        print(f"Error while running Enricher: {e}")

if __name__ == "__main__":
    main()