"""
Database Client Extensions for Conversation State Management

Extends DatabaseClient with state management methods without modifying core layer.
"""

import logging

logger = logging.getLogger(__name__)


class DatabaseClientExtensions:
    """Extension methods for DatabaseClient to handle conversation state."""
    
    @staticmethod
    def add_state_methods(db_client):
        """
        Add state management methods to existing DatabaseClient instance.
        
        Args:
            db_client: DatabaseClient instance to extend
        """
        if not db_client:
            return
        
        def get_conversation_state(sender: str) -> str:
            """
            Get conversation state for a user.

            Args:
                sender: WhatsApp number

            Returns:
                JSON string of state data or None
            """
            try:
                with db_client.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        SELECT state_data
                        FROM conversation_states
                        WHERE sender = %s
                        """,
                        (sender,)
                    )
                    row = cursor.fetchone()

                    if row:
                        logger.info(f"Retrieved conversation state for {sender} from SQL Server")
                        return row[0]
                    else:
                        logger.info(f"No conversation state found for {sender} in SQL Server")
                        return None

            except Exception as e:
                logger.error(f"Error retrieving conversation state from SQL Server: {e}", exc_info=True)
                return None

        def save_conversation_state(sender: str, state_json: str):
            """
            Save conversation state for a user.

            Args:
                sender: WhatsApp number
                state_json: JSON string of state data
            """
            try:
                from datetime import datetime, timezone

                with db_client.get_connection() as conn:
                    cursor = conn.cursor()

                    # Use MERGE to insert or update
                    cursor.execute(
                        """
                        MERGE INTO conversation_states AS target
                        USING (SELECT %s AS sender) AS source
                        ON target.sender = source.sender
                        WHEN MATCHED THEN
                            UPDATE SET state_data = %s, updated_at = %s
                        WHEN NOT MATCHED THEN
                            INSERT (sender, state_data, updated_at)
                            VALUES (%s, %s, %s);
                        """,
                        (sender, state_json, datetime.now(timezone.utc), sender, state_json, datetime.now(timezone.utc))
                    )
                    conn.commit()
                    logger.info(f"Saved conversation state for {sender} to SQL Server")

            except Exception as e:
                logger.error(f"Error saving conversation state to SQL Server: {e}", exc_info=True)

        def delete_conversation_state(sender: str):
            """
            Delete conversation state for a user.

            Args:
                sender: WhatsApp number
            """
            try:
                with db_client.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "DELETE FROM conversation_states WHERE sender = %s",
                        (sender,)
                    )
                    conn.commit()
                    logger.info(f"Deleted conversation state for {sender} from SQL Server")

            except Exception as e:
                logger.error(f"Error deleting conversation state from SQL Server: {e}", exc_info=True)
        
        # Add methods to db_client instance
        db_client.get_conversation_state = get_conversation_state
        db_client.save_conversation_state = save_conversation_state
        db_client.delete_conversation_state = delete_conversation_state
        
        logger.info("DatabaseClient extended with state management methods")
