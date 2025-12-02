"""
Conversation State Management for FG-Chatbot-03

Manages multi-turn conversation flow with state persistence.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class ConversationStage(Enum):
    """Conversation stages for state machine."""
    GREETING = "greeting"
    NAME_VERIFICATION = "name_verification"
    DOCUMENT_CHOICE = "document_choice"
    ACCOUNT_STATEMENT_CHOICE = "account_statement_choice"
    CONTRACT_ID_INPUT = "contract_id_input"
    FOLLOW_UP = "follow_up"
    ENDED = "ended"


class ConversationState:
    """Represents the state of a conversation with a user."""
    
    def __init__(self, sender: str):
        """
        Initialize conversation state.
        
        Args:
            sender: WhatsApp number of the user
        """
        self.sender = sender
        self.stage = ConversationStage.GREETING
        self.user_name = None
        self.verified = False
        self.verification_attempts = 0
        self.last_document_type = None
        self.awaiting_input = None
        self.pending_name_options = []
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)
    
    def to_dict(self) -> Dict:
        """Convert state to dictionary for storage."""
        return {
            "sender": self.sender,
            "stage": self.stage.value,
            "user_name": self.user_name,
            "verified": self.verified,
            "verification_attempts": self.verification_attempts,
            "last_document_type": self.last_document_type,
            "awaiting_input": self.awaiting_input,
            "pending_name_options": self.pending_name_options,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'ConversationState':
        """Create state from dictionary."""
        state = cls(data["sender"])
        state.stage = ConversationStage(data["stage"])
        state.user_name = data.get("user_name")
        state.verified = data.get("verified", False)
        state.verification_attempts = data.get("verification_attempts", 0)
        state.last_document_type = data.get("last_document_type")
        state.awaiting_input = data.get("awaiting_input")
        state.pending_name_options = data.get("pending_name_options", [])
        state.created_at = datetime.fromisoformat(data["created_at"])
        state.updated_at = datetime.fromisoformat(data["updated_at"])
        return state
    
    def update(self):
        """Update the timestamp."""
        self.updated_at = datetime.now(timezone.utc)


class ConversationStateManager:
    """Manages conversation states for all users."""
    
    def __init__(self, db_client=None):
        """
        Initialize state manager.
        
        Args:
            db_client: Optional DatabaseClient for persistence
        """
        self.db_client = db_client
        self._states = {}  # In-memory cache: {sender: ConversationState}
        logger.info("ConversationStateManager initialized")
    
    def get_state(self, sender: str) -> ConversationState:
        """
        Get or create conversation state for a user.
        
        Args:
            sender: WhatsApp number
        
        Returns:
            ConversationState instance
        """
        # Check memory cache first
        if sender in self._states:
            return self._states[sender]
        
        # Try to load from database
        if self.db_client:
            try:
                state_data = self.db_client.get_conversation_state(sender)
                if state_data:
                    state = ConversationState.from_dict(json.loads(state_data))
                    self._states[sender] = state
                    logger.info(f"Loaded state for {sender} from database: {state.stage.value}")
                    return state
            except Exception as e:
                logger.warning(f"Failed to load state from database: {e}")
        
        # Create new state
        state = ConversationState(sender)
        self._states[sender] = state
        logger.info(f"Created new state for {sender}")
        return state
    
    def save_state(self, state: ConversationState):
        """
        Save conversation state.
        
        Args:
            state: ConversationState to save
        """
        state.update()
        self._states[state.sender] = state
        
        # Persist to database if available
        if self.db_client:
            try:
                self.db_client.save_conversation_state(
                    state.sender,
                    json.dumps(state.to_dict())
                )
                logger.info(f"Saved state for {state.sender} to database: {state.stage.value}")
            except Exception as e:
                logger.warning(f"Failed to save state to database: {e}")
    
    def reset_state(self, sender: str):
        """
        Reset conversation state for a user.
        
        Args:
            sender: WhatsApp number
        """
        if sender in self._states:
            del self._states[sender]
        
        if self.db_client:
            try:
                self.db_client.delete_conversation_state(sender)
                logger.info(f"Reset state for {sender}")
            except Exception as e:
                logger.warning(f"Failed to delete state from database: {e}")
