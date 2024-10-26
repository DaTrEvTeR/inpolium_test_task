# **Test Task for Inpolium Data**

This project is a **web scraper** designed to parse all products from [igefa Store](https://store.igefa.de/). It includes an **intermediate dataset** to ensure that the scraper can resume from where it left off, preventing loss of progress.

---

## **Installation**

Make sure you have [Poetry](https://python-poetry.org/) installed. To install all project dependencies, run:

```shell
poetry install
```

Activate environment shell by:

```shell
poetry shell
```

## run

Use next command to run scrapper:

```shell
python main.py
```

## Dependencies

The project uses the following dependencies:

**aiohttp**: Asynchronous requests to the website  
**beautifulsoup4**: Parsing HTML content  
**lxml**: Parser for BeautifulSoup  
**aiosqlite**: Asynchronous SQLite database operations

## author

### Mykhailo Rozhkov

[LinkedIn](https://github.com/DaTrEvTeR)  
[Telegram](https://t.me/datrevter)  
Email: rozhkovm176@gmail.com
