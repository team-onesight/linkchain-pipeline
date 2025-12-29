UPSERT_TAG_SQL = """
    INSERT INTO public.tag (tag_name, created_at)
    SELECT DISTINCT
        st.tag_name,
        st.created_at
    FROM staging.tag st
    LEFT JOIN public.tag pt
    ON pt.tag_name = st.tag_name
    WHERE pt.tag_id IS NULL;
    """

TAG_MAPPING_SQL = """
    INSERT INTO public.link_tag_map (link_id, tag_id)
    SELECT
        st.link_id,
        pt.tag_id
    FROM staging.tag st
    JOIN public.tag pt
    ON pt.tag_name = st.tag_name
    LEFT JOIN public.link_tag_map m
    ON m.link_id = st.link_id
    AND m.tag_id = pt.tag_id
    WHERE m.link_id IS NULL;
    """

TAG_STAGING_COLUMNS = ["link_id", "tag_name", "created_at"]
