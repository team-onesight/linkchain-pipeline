MERGE_LINK_SQL = """
    MERGE INTO public.link pl
    USING staging.link sl
    ON pl.link_id = sl.link_id
    WHEN MATCHED THEN
        UPDATE SET
            title = sl.title,
            description = sl.description,
            link_embedding = sl.link_embedding
            image_url = sl.image_url
    WHEN NOT MATCHED THEN
        INSERT (
            link_id,
            url,
            title,
            description,
            created_by_user_id,
            created_by_username,
            created_at,
            link_embedding,
            image_url
        )
        VALUES (
            sl.link_id,
            sl.url,
            sl.title,
            sl.description,
            sl.created_by_user_id,
            sl.created_by_username,
            sl.created_at,
            sl.link_embedding,
            sl.image_url
        );
    """

LINK_STAGING_COLUMNS = ["link_id", "url", "title", "description", "created_by_user_id", "created_by_username", "created_at", "link_embedding"] # noqa: E501
