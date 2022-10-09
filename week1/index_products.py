# From https://github.com/dshvadskiy/search_with_machine_learning_course/blob/main/index_products.py
import opensearchpy
import requests
from lxml import etree

import click
import glob
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
import logging

from time import perf_counter
import concurrent.futures



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(format='%(levelname)s:%(message)s')

# NOTE: this is not a complete list of fields.  If you wish to add more, put in the appropriate XPath expression.
#TODO: is there a way to do this using XPath/XSL Functions so that we don't have to maintain a big list?
mappings =  [
            "productId/text()", "productId",
            "sku/text()", "sku",
            "name/text()", "name",
            "type/text()", "type",
            "startDate/text()", "startDate",
            "active/text()", "active",
            "regularPrice/text()", "regularPrice",
            "salePrice/text()", "salePrice",
            "artistName/text()", "artistName",
            "onSale/text()", "onSale",
            "digital/text()", "digital",
            "frequentlyPurchasedWith/*/text()", "frequentlyPurchasedWith",# Note the match all here to get the subfields
            "accessories/*/text()", "accessories",# Note the match all here to get the subfields
            "relatedProducts/*/text()", "relatedProducts",# Note the match all here to get the subfields
            "crossSell/text()", "crossSell",
            "salesRankShortTerm/text()", "salesRankShortTerm",
            "salesRankMediumTerm/text()", "salesRankMediumTerm",
            "salesRankLongTerm/text()", "salesRankLongTerm",
            "bestSellingRank/text()", "bestSellingRank",
            "url/text()", "url",
            "categoryPath/*/name/text()", "categoryPath", # Note the match all here to get the subfields
            "categoryPath/*/id/text()", "categoryPathIds", # Note the match all here to get the subfields
            "categoryPath/category[last()]/id/text()", "categoryLeaf",
            "count(categoryPath/*/name)", "categoryPathCount",
            "customerReviewCount/text()", "customerReviewCount",
            "customerReviewAverage/text()", "customerReviewAverage",
            "inStoreAvailability/text()", "inStoreAvailability",
            "onlineAvailability/text()", "onlineAvailability",
            "releaseDate/text()", "releaseDate",
            "shippingCost/text()", "shippingCost",
            "shortDescription/text()", "shortDescription",
            "shortDescriptionHtml/text()", "shortDescriptionHtml",
            "class/text()", "class",
            "classId/text()", "classId",
            "subclass/text()", "subclass",
            "subclassId/text()", "subclassId",
            "department/text()", "department",
            "departmentId/text()", "departmentId",
            "bestBuyItemId/text()", "bestBuyItemId",
            "description/text()", "description",
            "manufacturer/text()", "manufacturer",
            "modelNumber/text()", "modelNumber",
            "image/text()", "image",
            "condition/text()", "condition",
            "inStorePickup/text()", "inStorePickup",
            "homeDelivery/text()", "homeDelivery",
            "quantityLimit/text()", "quantityLimit",
            "color/text()", "color",
            "depth/text()", "depth",
            "height/text()", "height",
            "weight/text()", "weight",
            "shippingWeight/text()", "shippingWeight",
            "width/text()", "width",
            "longDescription/text()", "longDescription",
            "longDescriptionHtml/text()", "longDescriptionHtml",
            "features/*/text()", "features" # Note the match all here to get the subfields

        ]

def get_opensearch():
    host = 'localhost'
    port = 9200
    auth = ('admin', 'admin')
    #### Step 2.a: Create a connection to OpenSearch
    client = OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_compress = True, # enables gzip compression for request bodies
        http_auth = auth,
        # client_cert = client_cert_path,
        # client_key = client_key_path,
        use_ssl = True,
        verify_certs = False,
        ssl_assert_hostname = False,
        ssl_show_warn = False,
        # ca_certs = ca_certs_path
    )
    return client


def index_file(file, index_name):
    docs_indexed = 0
    client = get_opensearch()
    logger.info(f'Processing file : {file}')
    tree = etree.parse(file)
    root = tree.getroot()
    children = root.findall("./product")
    docs = []
    for child in children:
        doc = {}
        for idx in range(0, len(mappings), 2):
            xpath_expr = mappings[idx]
            key = mappings[idx + 1]
            doc[key] = child.xpath(xpath_expr)
        # print(doc)
        if 'productId' not in doc or len(doc['productId']) == 0:
            continue
        #### Step 2.b: Create a valid OpenSearch Doc and bulk index 2000 docs at a time
        the_doc = {
            "id": long(doc['productId'][0]),
            "_index": index_name,
            "productId": long(doc['productId'][0]),
            # "sku": doc['sku'][0],
            # "name": doc['name'][0],
            # "type": doc['type'][0],
            # "startDate": doc['startDate'][0],
            # "active": doc['active'][0],
            # "regularPrice": doc['regularPrice'][0],
            # "salePrice": doc['salePrice'][0],
            # "artistName": doc['artistName'][0],
            # "onSale": doc['onSale'][0],
            # "digital": doc['digital'][0],
            # "frequentlyPurchasedWith": doc['frequentlyPurchasedWith'],
            # "accessories": doc['accessories'],
            # "relatedProducts": doc['relatedProducts'],
            # "crossSell": doc['crossSell'][0],
            # "salesRankShortTerm": doc['salesRankShortTerm'][0],
            # "salesRankMediumTerm": doc['salesRankMediumTerm'][0],
            # "salesRankLongTerm": doc['salesRankLongTerm'][0],
            # "bestSellingRank": doc['bestSellingRank'][0],
            # "url": doc['url'][0],
            # "categoryPath": doc['categoryPath'],
            # "categoryPathIds": doc['categoryPathIds'],
            # "categoryLeaf": doc['categoryLeaf'][0],
            # "categoryPathCount": doc['categoryLeaf'][0],
            # "customerReviewCount": doc['customerReviewCount'][0],
            # "customerReviewAverage": doc['customerReviewAverage'][0],
            # "inStoreAvailability": doc['inStoreAvailability'][0],
            # "onlineAvailability": doc['onlineAvailability'][0],
            # "releaseDate": doc['releaseDate'][0],
            # "shippingCost": doc['shippingCost'][0],
            # "shortDescription": doc['shortDescription'][0],
            # "shortDescriptionHtml": doc['shortDescriptionHtml'][0],
            # "class": doc['class'][0],
            # "classId": doc['classId'][0],
            # "subclass": doc['subclass'][0],
            # "subclassId": doc['subclassId'][0],
            # "department": doc['department'][0],
            # "departmentId": doc['departmentId'][0],
            # "bestBuyItemId": doc['bestBuyItemId'][0],
            # "description": doc['description'][0],
            # "manufacturer": doc['manufacturer'][0],
            # "modelNumber": doc['modelNumber'][0],
            # "image": doc['image'][0],
            # "condition": doc['condition'][0],
            # "inStorePickup": doc['inStorePickup'][0],
            # "homeDelivery": doc['homeDelivery'][0],
            # "quantityLimit": doc['quantityLimit'][0],
            # "color": doc['color'][0],
            # "depth": doc['depth'][0],
            # "height": doc['height'][0],
            # "weight": doc['weight'][0],
            # "shippingWeight": doc['shippingWeight'][0],
            # "width": doc['width'][0],
            # "longDescription": doc['longDescription'][0],
            # "longDescriptionHtml": doc['longDescriptionHtml'][0],
            # "features": doc['features']
        }
        docs.append(the_doc)
        print("1111", len(docs), the_doc)
        if len(docs) == 2000:
            print("HERE HERE HERE")
            print(bulk(client, docs))
            print(client.cat.count(index_name, params={"v": "true"}))
            docs_indexed += len(docs)
            docs = []

    return docs_indexed

@click.command()
@click.option('--source_dir', '-s', help='XML files source directory')
@click.option('--index_name', '-i', default="bbuy_products", help="The name of the index to write to")
@click.option('--workers', '-w', default=8, help="The number of workers to use to process files")
def main(source_dir: str, index_name: str, workers: int):

    files = glob.glob(source_dir + "/*.xml")
    docs_indexed = 0
    start = perf_counter()
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(index_file, file, index_name) for file in files]
        for future in concurrent.futures.as_completed(futures):
            docs_indexed += future.result()

    finish = perf_counter()
    logger.info(f'Done. Total docs: {docs_indexed} in {(finish - start)/60} minutes')

if __name__ == "__main__":
    main()
