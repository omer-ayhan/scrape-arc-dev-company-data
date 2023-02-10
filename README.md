# Scrape script

## Installation

if you don't have pipenv installed, install it first

```bash
pip install pipenv
```

then install the dependencies

```bash
pipenv install
```

check out `.env.example` and create a `.env` file with the correct values

```bash
cp .env.example .env
```

## Usage

```bash
pipenv run scrape
```

to run in threaded mode

```bash
pipenv run scrape-threaded
```

## Author

[Omer Ayhan](https://github.com/omer-ayhan)
