import xml.etree.ElementTree as ET
import pandas as pd
from sqlalchemy import create_engine, inspect
import requests
import logging
import zipfile
import io
import sys
import re

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def clean_name(name):
    if not name:
        return ""
    return re.sub(r"\bpty\s*\.?\s*ltd\b", "", name, flags=re.IGNORECASE).strip().lower()

def table_exists(engine, table_name, schema=None):
    inspector = inspect(engine)
    return inspector.has_table(table_name, schema=schema)

def extract_abr():
    try:
        logger.info(f"Pandas {pd.__version__}, SQLAlchemy {sys.modules['sqlalchemy'].__version__}")
        engine = create_engine('postgresql+psycopg2://etl_user:secure_password@localhost:5432/company_db', echo=False)
        if not table_exists(engine, "raw_abr"):
            raise Exception("Table raw_abr does not exist")

        zip_url = "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/635fcb95-7864-4509-9fa7-a62a6e32b62d/download/public_split_11_20.zip"
        response = requests.get(zip_url)
        response.raise_for_status()
        logger.info("Downloaded ABR ZIP")

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            xml_files = [f for f in z.namelist() if f.endswith('.xml')]
            logger.info(f"Found {len(xml_files)} XML files")
            for xml_file in xml_files:
                logger.info(f"Processing {xml_file}")
                with z.open(xml_file) as file:
                    records = []
                    try:
                        tree = ET.parse(file)
                        root = tree.getroot()
                        for abr_entity in root.findall(".//ABR"):
                            abn_node = abr_entity.find("ABN")
                            if abn_node is None:
                                continue
                            abn = abn_node.text.strip()
                            entity_status = abn_node.attrib.get("status", "").strip()
                            start_date = abn_node.attrib.get("ABNStatusFromDate", "").strip()
                            entity_type = abr_entity.findtext("EntityType/EntityTypeInd", default="").strip()
                            entity_name = ""
                            main_entity = abr_entity.find("MainEntity")
                            legal_entity = abr_entity.find("LegalEntity")
                            if main_entity is not None:
                                entity_name = main_entity.findtext("NonIndividualName/NonIndividualNameText", default="").strip()
                            elif legal_entity is not None:
                                given_names = legal_entity.findall("IndividualName/GivenName")
                                family = legal_entity.findtext("IndividualName/FamilyName", default="").strip()
                                given_combined = ' '.join([g.text.strip() for g in given_names if g.text])
                                entity_name = f"{given_combined} {family}".strip()
                            address_node = main_entity or legal_entity
                            address_details = address_node.find("BusinessAddress/AddressDetails") if address_node is not None else None
                            postcode = address_details.findtext("Postcode", default="").strip() if address_details is not None else ""
                            state = address_details.findtext("State", default="").strip() if address_details is not None else ""
                            flat = address_details.findtext("FlatNumber", default="").strip() if address_details is not None else ""
                            street = address_details.findtext("StreetName", default="").strip() if address_details is not None else ""
                            suburb = address_details.findtext("Suburb", default="").strip() if address_details is not None else ""
                            address_parts = [flat, street, suburb]
                            address = ', '.join(part for part in address_parts if part)
                            # Skip if no ABN or name
                            if not abn or not entity_name:
                                continue
                            records.append({
                                "abn": abn,
                                "entity_name": clean_name(entity_name),
                                "entity_type": entity_type,
                                "entity_status": entity_status,
                                "address": address.strip().lower(),
                                "postcode": postcode.strip(),
                                "state": state.strip().upper(),
                                "start_date": start_date
                            })
                    except ET.ParseError as e:
                        logger.error(f"XML parsing error in {xml_file}: {e}")
                        continue

                    if records:
                        df = pd.DataFrame(records)
                        df = df[df['abn'].str.match(r'^\d{11}$')]
                        df = df.drop_duplicates(subset="abn", keep="last")

                        with engine.begin() as conn:
                            df.to_sql("raw_abr", con=conn, if_exists="append", index=False, chunksize=1000, method="multi")
                        logger.info(f"Inserted {len(df)} clean, deduped records from {xml_file}")
                    else:
                        logger.warning(f"No valid records in {xml_file}")

    except Exception as e:
        logger.error(f"ABR extraction failed: {e}")
    finally:
        if 'engine' in locals():
            engine.dispose()
            logger.info("Disposed SQLAlchemy engine")

if __name__ == '__main__':
    extract_abr()
