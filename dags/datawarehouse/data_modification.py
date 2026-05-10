import logging
from sre_constants import IN

logger = logging.getLogger(__name__)
table ='yt_api'

def insert_rows(cur, conn, schema, row):
    try:
        if schema == 'staging':
            video_id = 'videoId'
            cur.execute(f"""
                INSERT INTO {schema}.{table} ("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(videoId)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s)
                ON CONFLICT ("Video_ID") DO NOTHING;
            """,row
            )
        else:
            video_id = 'Video_ID'
            cur.execute(f"""
                INSERT INTO {schema}.{table} ("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s)
                ON CONFLICT ("Video_ID") DO NOTHING;
            """,row
            )
        conn.commit()
        logger.info(f"Inserted video ID: {row[video_id]} into {schema}.{table}")
    except Exception as e:
        logger.error(f"Error inserting video ID: {row[video_id]} into {schema}.{table} - {e}")
        raise e

def update_rows(cur, conn, schema, row):
    try:
        if schema == 'staging':
            video_id = 'videoId'
            upload_date = 'publishedAt'
            video_title = 'title'
            video_views = 'viewCount'
            likes_count = 'likeCount'
            comments_count = 'commentCount'
           
        else:
            video_id = 'Video_ID'
            upload_date = 'Upload_Date'
            video_title = 'Video_Title'
            video_views = 'Video_Views'
            likes_count = 'Likes_Count'
            comments_count = 'Comments_Count'
        
        cur.execute(f"""
            UPDATE {schema}.{table}
            SET "Video_Title" = %({video_title})s,
                "Upload_Date" = %({upload_date})s,
                "Video_Views" = %({video_views})s,
                "Likes_Count" = %({likes_count})s,
                "Comments_Count" = %({comments_count})s
            WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
        """,row)
        conn.commit()
        logger.info(f"Updated video ID: {row[video_id]} in {schema}.{table}")
    except Exception as e:
        logger.error(f"Error updating video ID: {row[video_id]} in {schema}.{table} - {e}")
        raise e
    
def delete_rows(cur, conn, schema, ids_to_delete):
    try:
        ids_to_delete = f"""({', '.join(f"'{id}'" for id in ids_to_delete)})"""
        
        cur.execute(f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" IN {ids_to_delete};
        """
        )
        
        conn.commit()
        logger.info(f"Deleted video ID: {ids_to_delete} from {schema}.{table}")
    except Exception as e:
        logger.error(f"Error deleting video ID: {ids_to_delete} from {schema}.{table} - {e}")
        raise e