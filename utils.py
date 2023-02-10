def retry(howmany, message="Retrying..."):
    def tryIt(func):
        def f():
            attempts = 0
            while attempts < howmany:
                try:
                    print(message)
                    return func()
                except:
                    print(f"Failed after {attempts} attempts.")
                    attempts += 1

        return f

    return tryIt
