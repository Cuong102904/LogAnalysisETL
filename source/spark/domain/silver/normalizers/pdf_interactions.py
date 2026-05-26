from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_pdf_interactions(df: DataFrame) -> DataFrame:
    """
    Normalize PDF/textbook and Google Doc interactions.
    Sources:
      - textbook.pdf.page.scrolled   -> event: {page, direction, chapter, name}
      - textbook.pdf.display.scaled  -> event: {amount, page, chapter, name}
      - textbook.pdf.zoom.buttons.changed -> event: {amount, page, chapter, name}
      - book                         -> event: {chapter, page}
      - edx.googlecomponent.document.displayed -> event: {url, displayed_in}
    """
    base = df.filter(F.col("silver_class") == "pdf")
    ev = F.from_json(F.get_json_object("value_raw", "$.event"), "map<string,string>")

    return base.select(
        F.col("dedup_key").alias("event_id"),
        F.col("time").alias("time"),
        F.to_date(F.col("time")).alias("event_date"),
        F.get_json_object("value_raw", "$.event_type").alias("event_type"),
        F.get_json_object("value_raw", "$.username").alias("username"),
        F.get_json_object("value_raw", "$.context.user_id").cast("long").alias("user_id"),
        F.get_json_object("value_raw", "$.session").alias("session_id"),
        F.get_json_object("value_raw", "$.context.course_id").alias("course_id"),
        F.get_json_object("value_raw", "$.context.org_id").alias("org_id"),
        # PDF fields (from raw event payload)
        ev.getItem("chapter").alias("chapter"),
        ev.getItem("name").alias("pdf_name"),
        ev.getItem("page").cast("int").alias("page_number"),
        ev.getItem("direction").alias("scroll_direction"),  # 'up'|'down', null otherwise
        ev.getItem("amount").cast("double").alias("scale_amount"),  # null unless scaled
        # Google Doc fields
        ev.getItem("url").alias("doc_url"),  # null unless google_doc
    )
