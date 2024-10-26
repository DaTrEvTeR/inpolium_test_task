import aiohttp
import asyncio
import csv
import os
import re
import logging

from bs4 import BeautifulSoup

from constants import BASE_URL, BASE_API_URL, BASE_PRODUCT_URL, DATABASE
from db import Database
from custom_session import CustomSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


async def get_categories() -> list[str]:
    """Retrieve category URLs.

    Returns
    -------
    list[str]
        List of category URLs.
    """
    async with CustomSession() as session:
        try:
            async with session.get(BASE_URL) as response:
                if response.status != 200:
                    raise aiohttp.ClientError(
                        f"Ошибка {response.status}: {response.reason}"
                    )
                html = await response.text()
                soup = BeautifulSoup(html, "lxml")
                categories = soup.select(
                    "div.ant-col.ant-col-xs-24.ant-col-sm-12.ant-col-lg-7 a", limit=12
                )
                category_urls = [cat["href"] for cat in categories]
                logging.info(f"Retrieved categories: {category_urls}")
                return category_urls
        except Exception as e:
            logging.error(f"Error retrieving categories: {e}")
            return []


async def process_category(category_url: str, start_page: int, db: Database) -> None:
    """Process a specific category starting from the specified page.

    Parameters
    ----------
    category_url : str
        The URL of the category to process.
    start_page : int
        The page number to start processing from.
    """
    category_code = category_url.split("/")[-1]
    category_name = category_url.split("/")[-2]
    category_api_url = f"{BASE_API_URL}filter%5Btaxonomy%5D={category_code}&limit=0"
    page_count = await get_category_page_count(category_api_url)

    for page_num in range(start_page, page_count + 1):
        await asyncio.sleep(1)
        logging.info(f"Processing {category_name}, page {page_num}...")
        try:
            page_products = await get_page_products(category_code, page_num)
            products_data = await db.get_processed_products(category_code, page_num)
            products_urls = await get_products_urls(page_products, products_data)
            html_pages = await fetch_all_products(products_urls)

            for html, product_url in zip(html_pages, products_urls):
                product_data = await parse_product_data(html, product_url)
                await db.add_to_processed(product_data, category_code, str(page_num))
                products_data.append(product_data)

            await save_to_csv(category_name, page_num, products_data)
        except Exception as e:
            logging.error(
                f"Error processing category {category_code}, page {page_num}: {e}"
            )


async def get_category_page_count(category_api_url: str) -> int:
    """Get the total number of pages for a given category.

    Parameters
    ----------
    category_api_url : str
        The API URL for the category.

    Returns
    -------
    int
        The total number of pages.
    """
    async with CustomSession() as session:
        try:
            async with session.get(category_api_url) as response:
                if not response.ok:
                    raise aiohttp.ClientError(
                        f"Error {response.status}: {response.reason}"
                    )
                data: dict = await response.json(encoding="utf-8")
                products_in_category_count = data["total"]
                total_pages = (products_in_category_count + 19) // 20
                logging.info(
                    f"Total pages for category {category_api_url.split('=')[-1]}: {total_pages}"
                )
                return total_pages
        except Exception as e:
            logging.error(f"Error retrieving page count: {e}")
            return 0


async def get_page_products(category_article: str, page_num: int) -> dict:
    """Get products for a specific page in a category.

    Parameters
    ----------
    category_article : str
        The category identifier.
    page_num : intcategory_code
    Returns
    -------
    dict
        The JSON response containing product data.
    """
    async with CustomSession() as session:
        url = f"{BASE_API_URL}filter%5Btaxonomy%5D={category_article}&limit=20&page={page_num}"
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    raise aiohttp.ClientError(
                        f"Ошибка {response.status}: {response.reason}"
                    )
                res = await response.json()
                logging.info(f"Gettedd data from {url}")
                return res
        except Exception as e:
            logging.error(f"Error retrieving products from page {page_num}: {e}")
            return {}


async def get_products_urls(
    page_products: dict, processed_products: list[dict[str, str]]
) -> list[str]:
    """Get product URLs from the page products data.

    Parameters
    ----------
    page_products : dict
        The product data from the page.
    processed_products : list[str]
        List of already processed product URLs.

    Returns
    -------
    list[str]
        List of product URLs to be processed.
    """
    result = []
    processsed_urls = [product["supplier_url"] for product in processed_products]
    for item in page_products.get("hits", []):
        product_url = f"{BASE_PRODUCT_URL}{item['mainVariant']['slug']}/{item['mainVariant']['id']}"
        if product_url not in processsed_urls:
            result.append(product_url)
    logging.info(f"Found product URLs: {result}, total: {len(result)}")
    return result


async def fetch_all_products(
    products_urls: list[str], max_connections: int = 5
) -> list[str]:
    """Fetch HTML for all product URLs concurrently.

    Parameters
    ----------
    products_urls : list[str]
        List of product URLs to fetch.
    max_connections : int, optional
        Maximum number of concurrent connections, by default 5.

    Returns
    -------
    list[str]
        List of HTML content for each product URL.
    """
    semaphore = asyncio.Semaphore(max_connections)
    async with CustomSession() as session:
        tasks = [fetch(semaphore, session, url) for url in products_urls]
        return await asyncio.gather(*tasks)


async def fetch(semaphore, session, url) -> str:
    """Fetch the content of a given URL using an asynchronous HTTP GET request.

    Parameters
    ----------
    semaphore : asyncio.Semaphore
        A semaphore to limit the number of concurrent requests.
    session : aiohttp.ClientSession
        The session used to perform the HTTP request.
    url : str
        The URL to fetch.

    Returns
    -------
    str
        The HTML content of the requested URL.
    """
    async with semaphore:
        async with session.get(url) as response:
            if not response.ok:
                raise aiohttp.ClientError(
                    f"Ошибка {response.status}: {response.reason}"
                )
            return await response.text()


async def parse_product_data(html: str, url: str) -> dict:
    """Parse product data from the given HTML content.

    Parameters
    ----------
    html : str
        The HTML content of the product page.
    url : str
        The URL of the product page.

    Returns
    -------
    dict
        A dictionary containing parsed product information, including:
        - product_name
        - original_data_column_1
        - original_data_column_2
        - supplier_article_number
        - ean
        - article_number
        - product_description
        - supplier
        - supplier_url
        - product_image_url
        - manufacturer
        - original_data_column_3
    """
    soup = BeautifulSoup(html, "lxml")

    product_name = soup.select_one(
        "h1.ant-typography.LYSTypography_h3__dfd45.ProductInformation_productTitle__61297"
    )
    if product_name:
        product_name = product_name.text

    original_data_column_1 = soup.select(
        "div.CategoryBreadcrumbs_sectionWrap__b5732 span > a"
    )
    if original_data_column_1:
        original_data_column_1 = (
            "/".join(el.text for el in original_data_column_1).lower().replace(" ", "-")
        )

    elements = soup.select("div.ProductInformation_variantInfo__5cb1d div")
    data = {
        el.get_text(strip=True).split(":")[0]: el.get_text(strip=True).split(":")[1]
        for el in elements
        if ":" in el.get_text(strip=True)
    }

    product_description = soup.select_one(
        "div.ant-card.ProductDescription_descriptionBox__90c31.ProductDetail_description__929ca div p"
    )
    if product_description:
        product_description = product_description.text

    product_image_url = soup.select_one("img.image-gallery-image")
    if product_image_url:
        product_image_url = product_image_url["src"]

    manufacturer = soup.find("td", string=re.compile(r".*Hersteller.*"))
    if manufacturer:
        manufacturer = manufacturer.find_next("td").text.strip()

    original_data_column_3 = soup.select(
        "div.ProductBenefits_productBenefits__1b77a li"
    )
    if original_data_column_3:
        original_data_column_3 = "; ".join(item.text for item in original_data_column_3)

    return {
        "product_name": product_name if product_name else None,
        "original_data_column_1": original_data_column_1
        if original_data_column_1
        else None,
        "original_data_column_2": data.get("Ausführung", None),
        "supplier_article_number": data.get("Artikelnummer", None),
        "ean": data.get("EAN", None),
        "article_number": data.get("Herstellernummer", None),
        "product_description": product_description if product_description else None,
        "supplier": "igefa Handelsgesellschaft",
        "supplier_url": url if url else None,
        "product_image_url": product_image_url if product_image_url else None,
        "manufacturer": manufacturer if manufacturer else None,
        "original_data_column_3": original_data_column_3
        if original_data_column_3
        else None,
    }


async def save_to_csv(category_name: str, page_num: int, data: list[dict]) -> None:
    """Save the product data to a CSV file.

    Parameters
    ----------
    category_name : str
        The name of the category for which data is being saved.
    page_num : int
        The page number of the data being saved.
    data : list[dict]
        A list of dictionaries containing product data to be written to the CSV file.
    """
    os.makedirs(f"data/{category_name}", exist_ok=True)
    file_path = f"data/{category_name}/page_{page_num}.csv"

    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


async def save_all_to_csv(db: Database) -> None:
    """Save all product data from the database (excluding 'category' and 'page') to a CSV file."""
    file_path = "data/all_data.csv"

    rows, columns = await db.get_all_from_db()

    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows([dict(zip(columns, row)) for row in rows])


async def main() -> None:
    """Main function to initialize the database and process product categories.

    This function performs the following steps:
    1. Initializes the database.
    2. Retrieves the last processed category and page number.
    3. Fetches the list of categories to process.
    4. Loops through the categories and processes each one.
    """
    db = Database(DATABASE)
    await db.init_db()
    last_category, last_page = await db.get_last_state()
    categories = await get_categories()

    for category_url in categories:
        if last_category and category_url.split("/")[-1] != last_category:
            continue
        await process_category(category_url, int(last_page), db)
        last_category = ""
        last_page = 1

    await save_all_to_csv(db)
    await db.clear_processed_urls()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as ex:
        print(ex)
