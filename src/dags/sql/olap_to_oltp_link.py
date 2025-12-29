MERGE_LINK_SQL = """
    MERGE INTO public.link pl
    USING staging.link sl
    ON pl.link_id = sl.link_id
    WHEN MATCHED THEN
        UPDATE SET
            title = sl.title,
            description = sl.description,
            is_fetched = true,
            link_embedding = sl.link_embedding
    WHEN NOT MATCHED THEN
        INSERT (
            link_id,
            url,
            title,
            description,
            created_by,
            created_at,
            is_fetched,
            link_embedding
        )
        VALUES (
            sl.link_id,
            sl.url,
            sl.title,
            sl.description,
            sl.created_by,
            sl.created_at,
            false,
            sl.link_embedding
        );
    """

LINK_STAGING_COLUMNS = ["link_id", "url", "title", "description", "created_by", "created_at", "link_embedding"] # noqa: E501
