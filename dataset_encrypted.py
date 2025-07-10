import os
import sys
import logging
import re
import csv
from pathlib import Path
import pandas as pd
import pyffx

# ── 0) Environment & Logging Setup ─────────────────────────────────────────────
# Ensure base data directory is set (fallback to current if missing)
os.environ.setdefault("DISSERTATION_DATA", "/home/roopam/Downloads/RDDissertation")
logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Allow reading very large CSV fields
csv.field_size_limit(sys.maxsize)

# ── Constants ──────────────────────────────────────────────────────────────────
# Filename of your sampled subset
EMAILS_SUBSET_FILENAME = "emails_1000.csv"
# Full path by joining base folder and filename
#EMAILS_SUBSET_CSV = str(Path(os.environ["DISSERTATION_DATA"]) / EMAILS_SUBSET_FILENAME)
EMAILS_SUBSET_CSV = EMAILS_SUBSET_FILENAME
# Output file for encrypted results
OUTPUT_CSV = "emails_1000_encrypted.csv"


# ── 1) Read Dataset ─────────────────────────────────────────────────────────────
def readdataset(path: str) -> pd.DataFrame:
    """
    Load the Enron emails CSV into a DataFrame.
    Expects at least columns: 'file' and 'message'.
    """
    csv_path = Path(path)
    if not csv_path.is_file():
        logging.error(f"CSV file not found: {csv_path}")
        sys.exit(1)
    try:
        df = pd.read_csv(csv_path, encoding="utf-8", engine="python")
        logging.info(f"Loaded dataset from {csv_path} ({len(df):,} rows)")
        return df
    except Exception as e:
        logging.exception(f"Failed to read CSV: {e}")
        sys.exit(1)


# ── 2) Parse Email ──────────────────────────────────────────────────────────────
def parse_email(row: pd.Series) -> pd.Series:
    """
    Split the raw 'message' text into:
      - 'file'      : original file identifier
      - 'Headers'   : all header lines up to the first blank line
      - 'Body'      : text after the first blank line, with spaces/tabs collapsed
    """
    raw = row["message"]                                   # full raw email text
    text = raw.replace("\r\n", "\n")                       # normalize CRLF → LF
    parts = text.split("\n\n", 1)                          # split at first blank line
    headers = parts[0]                                     # everything before blank line
    body    = parts[1] if len(parts) > 1 else ""            # rest is body
    # collapse multiple spaces/tabs into one, strip edges
    body_clean = re.sub(r"[\t ]+", " ", body).strip()
    return pd.Series({"file": row["file"], "Headers": headers, "Body": body_clean})


# ── 3) Clean Body ───────────────────────────────────────────────────────────────
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trim whitespace in 'Body', collapse runs of spaces, periods, and blank lines.
    Leaves 'Headers' untouched.
    """
    if "Body" in df.columns:
        # Use .loc to avoid SettingWithCopyWarning
        df.loc[:, "Body"] = (
            df["Body"]
            .fillna("")                                   # replace NaN with empty string
            .astype(str)                                  # ensure all are strings
            .str.replace(r"[ \t]+", " ", regex=True)      # collapse spaces/tabs
            .str.strip()                                  # trim leading/trailing spaces
            .str.replace(r"\.{2,}", ".", regex=True)      # collapse multiple periods
            .str.replace(r"\n{3,}", "\n\n", regex=True)   # collapse 3+ newlines to 2
        )
    return df


# ── 4) Load PII Pipeline ────────────────────────────────────────────────────────
def load_pii_pipeline():
    """
    Initialize and return a Hugging Face PII detection pipeline.
    Exits if the 'transformers' library or model fails to load.
    """
    try:
        from transformers import pipeline
    except ImportError:
        logging.error("transformers library not found. Install with 'pip install transformers'.")
        sys.exit(1)

    try:
        pipe = pipeline(
            "token-classification",
            model="ab-ai/pii_model",
            aggregation_strategy="simple"
        )
        logging.info("Loaded PII pipeline: ab-ai/pii_model")
        return pipe
    except Exception as e:
        logging.exception(f"Failed to load PII pipeline: {e}")
        sys.exit(1)


# ── 5) Identify PII ─────────────────────────────────────────────────────────────
def identify_pii(df: pd.DataFrame, text_column: str = "Body") -> pd.DataFrame:
    """
    Apply the PII pipeline to the specified text column.
    Adds a new column 'pii_entities' with the list of detected entities.
    """
    pipe = load_pii_pipeline()                             # load HF pipeline once
    df = df.copy()                                         # avoid modifying input directly
    df["pii_entities"] = df[text_column].apply(lambda txt: pipe(txt))
    logging.info(f"Detected PII in {len(df):,} records")
    return df

def fix_apostrophes(entities):
    fixed = []
    for ent in entities:
        w = ent["word"]
        # replace space–apostrophe–space (or variants) with a single apostrophe
        w_fixed = re.sub(r"\s*'\s*", "'", w)
        ent["word"] = w_fixed
        fixed.append(ent)
    return fixed


def add_speaker_fallback_anywhere(df, text_column="Body", ent_column="pii_entities"):
    """
    Inject FIRSTNAME entities for any α-token followed by one of [:, -, --, ->, /, !],
    case-insensitive, anywhere in the text.
    """
    # (?i) = ignore case, (?<!\w) = not preceded by a word‐char,
    # ([A-Za-z]+) = one or more letters,
    # (?=(?:!|--|->|[:\-/])) = lookahead for !, --, ->, :, -, or /
    pattern = re.compile(
        r'(?i)(?<!\w)([A-Za-z]+)(?=(?:!|--|->|[:\-/]))'
    )

    new_entity_lists = []
    for ents, text in zip(df[ent_column], df[text_column]):
        seen = { e["word"].lower() for e in ents }
        for m in pattern.finditer(text):
            name = m.group(1)  # e.g. "bill" from "bill!" or "Bill-" etc.
            if name.lower() not in seen:
                ents.append({
                    "entity_group": "FIRSTNAME",
                    "word":         name,
                    "start":        m.start(1),
                    "end":          m.end(1),
                    "score":        None,
                    "valid":        True
                })
                seen.add(name.lower())
        new_entity_lists.append(ents)

    df[ent_column] = new_entity_lists
    return df

# ── 6) Validate Entities ────────────────────────────────────────────────────────
def firstname_validate(name: str) -> bool:
    """
    Return True if `name` (after stripping trailing punctuation):
      - Starts with a letter
      - Contains at least two letters total
      - May include internal hyphens or apostrophes
    """
    clean = re.sub(r"[.,;:!?]+$", "", name)               # strip trailing .,;:!?
    # regex: letter + letter+ + (optional -'/letter+ groups)
    return bool(re.fullmatch(r"^[A-Za-z]+(?:['-][A-Za-z]+)*$", clean))


def lastname_validate(name: str) -> bool:
    """
    Same rules as first names.
    """
    clean = re.sub(r"[.,;:!?]+$", "", name)
    return bool(re.fullmatch(r"^[A-Za-z]+(?:['-][A-Za-z]+)*$", clean))


def validate_entities(df: pd.DataFrame,
                      entities_col: str = "pii_entities") -> pd.DataFrame:
    """
    For each row, copy the 'pii_entities' list and:
      - Annotate each dict with a 'valid' boolean
      - Leaves the original 'pii_entities' column unchanged
      - Stores results in 'pii_entities_validated'
    """
    all_validated = []
    for ents in df[entities_col]:
        row_valid = []
        for ent in ents:
            grp  = ent.get("entity_group", "")
            word = ent.get("word", "")
            if grp == "FIRSTNAME":
                ent["valid"] = firstname_validate(word)
            elif grp == "LASTNAME":
                ent["valid"] = lastname_validate(word)
            else:
                ent["valid"] = True                     # other types always True
            row_valid.append(ent)                       # keep all entries
        all_validated.append(row_valid)

    out = df.copy()                                      # work on a fresh DataFrame
    out["pii_entities_validated"] = all_validated        # append validated column
    logging.info("Added validation flags to PII entities")
    return out


# ── 7) Encrypt PII ──────────────────────────────────────────────────────────────
# FPE setup: secret key and alphabet
# ── FPE Setup ───────────────────────────────────────────────────────────────────
FPE_KEY = b"supersecretkey!"
FPE_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-'"
_cipher_cache = {}


def get_cipher(length: int) -> pyffx.String:
    """
    Return (and cache) a format-preserving cipher for strings of the given length.
    """
    if length not in _cipher_cache:
        _cipher_cache[length] = pyffx.String(FPE_KEY, FPE_ALPHABET, length)
    return _cipher_cache[length]


def encrypt_name(token: str) -> str:
    """
    Encrypt a FIRSTNAME or LASTNAME token via FPE:
      1. Strip non-letters at the edges
      2. If nothing remains, return the original
      3. Otherwise, encrypt the core and return it
    """
    # strip leading/trailing non-letters (commas, periods, etc.)
    core = re.sub(r"^[^A-Za-z]+|[^A-Za-z]+$", "", token)
    if not core:
        return token
    cipher = get_cipher(len(core))
    return cipher.encrypt(core)


# Placeholders for future encryptors:
# def encrypt_cardnumber(token: str) -> str: ...
# def encrypt_phone(token: str) -> str: ...


def encrypt_pii(df: pd.DataFrame) -> pd.DataFrame:
    """
    Walk through df['pii_entities_validated']/df['Body'], and for each entity:
     - If FIRSTNAME/LASTNAME and valid => encrypt_name()
     - (In future) If CARDNUMBER => encrypt_cardnumber(), etc.
     - Else => leave original token
    Builds:
      * df['pii_encrypted']   : list of {entity_group, original, encrypted}
      * df['encrypted_body']  : Body with tokens replaced
    """
    encrypted_entities = []
    encrypted_bodies = []

    for _, row in df.iterrows():
        ents = row["pii_entities_validated"]
        body = row["Body"]
        row_enc = []

        for ent in ents:
            grp, orig = ent["entity_group"], ent["word"]

            # dispatch to the correct encrypt_* function
            if ent.get("valid") and grp in ("FIRSTNAME", "LASTNAME"):
                enc = encrypt_name(orig)

            # stub for future PII types:
            # elif ent.get("valid") and grp == "CARDNUMBER":
            #     enc = encrypt_cardnumber(orig)
            # elif ent.get("valid") and grp == "PHONE":
            #     enc = encrypt_phone(orig)

            else:
                # everything else stays as-is
                enc = orig

            # record mapping
            row_enc.append({
                "entity_group": grp,
                "original": orig,
                "encrypted": enc
            })

            # if we generated ciphertext (i.e. enc != orig), replace in body
            if enc != orig:
                core = re.sub(r"^[^A-Za-z]+|[^A-Za-z]+$", "", orig)
                body = re.sub(rf"\b{re.escape(core)}\b", enc, body, flags=re.IGNORECASE)

        encrypted_entities.append(row_enc)
        encrypted_bodies.append(body)

    out = df.copy()
    out["pii_encrypted"] = encrypted_entities
    out["encrypted_body"] = encrypted_bodies
    return out

# ── Main Workflow ──────────────────────────────────────────────────────────────
def main():
    # 1) Load raw sample
    df_raw      = readdataset(EMAILS_SUBSET_CSV)

    # 2) Parse headers & body
    df_parsed   = df_raw.apply(parse_email, axis=1)

    # 3) Clean the Body text
    df_cleaned  = clean_dataframe(df_parsed)

    # 4) Detect PII in cleaned bodies
    df_pii      = identify_pii(df_cleaned, text_column="Body")
    df_pii["pii_entities"] = df_pii["pii_entities"].apply(fix_apostrophes)

    #df_ner = add_speaker_fallback_anywhere(
     #   df_pii,
     #   text_column="Body",  # the raw text you want to scan
     #   ent_column="pii_entities"  # the list to append into
    #)

    # 5) Validate first/last names only
    #df_validated= validate_entities(df_ner)
    df_validated = validate_entities(df_pii)

    # 6) Encrypt valid name entities and update body
    df_encrypted= encrypt_pii(df_validated)

    # 7) Save full results, preserving all intermediate and new columns
    df_encrypted.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Done. Encrypted output saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
