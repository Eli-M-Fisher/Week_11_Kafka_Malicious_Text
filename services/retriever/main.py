from retriever import Retriever

def main():
    """
    are is entry point for the Retriever service.
    """
    print("Retriever service is starting...")

    try:
        retriever = Retriever()
        retriever.run()
    except Exception as e:
        print(f"Error while running Retriever: {e}")

if __name__ == "__main__":
    main()