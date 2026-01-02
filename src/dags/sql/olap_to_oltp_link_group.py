UPSERT_LINK_GROUP_SQL = """
    INSERT INTO public.link_group (group_title, created_at)
    SELECT
        sg.group_title,
        MIN(sg.created_at) AS created_at
    FROM staging.link_group sg
    LEFT JOIN public.link_group pg
        ON pg.group_title = sg.group_title
    WHERE pg.group_id IS NULL
    GROUP BY sg.group_title;
    """

LINK_GROUP_MAPPING_SQL = """
    INSERT INTO public.link_group_link_map (link_id, group_id, created_at)
    SELECT
        sg.link_id,
        pg.group_id,
        sg.created_at
    FROM staging.link_group sg
    JOIN public.link_group pg
    ON pg.group_title = sg.group_title
    LEFT JOIN public.link_group_link_map m
    ON m.link_id = sg.link_id
    AND m.group_id = pg.group_id
    WHERE m.link_id IS NULL;
    """

LINK_GROUP_STAGING_COLUMNS = ["link_id", "group_title", "created_at"]
