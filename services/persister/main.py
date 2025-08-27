from persister import Persister

def main():
    """
    here is entry point for the Persister service.
    """
    print("Persister service is starting...")

    try:
        persister = Persister()
        persister.run()
    except Exception as e:
        print(f"Error while running Persister: {e}")

if __name__ == "__main__":
    main()