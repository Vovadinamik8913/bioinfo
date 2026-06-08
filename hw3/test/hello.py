from prefect import flow, task
import random

@task
def fetch(name: str) -> str:
    return f"hello, {name}"
@flow
def greet(names: list[str]):
    for n in names:
        print(fetch(n))
if __name__ == "__main__":
    greet([f"user{n}" for n in random.choices(range(50), k=10)])