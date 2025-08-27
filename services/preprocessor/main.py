from processor import Preprocessor

def main():
    """
    the entry point for the Preprocessor service.
    """
    print("Preprocessor service is starting...")

    try:
        pre = Preprocessor()
        pre.run()
    except Exception as e:
        print(f"Error while running Preprocessor: {e}")

if __name__ == "__main__":
    main()