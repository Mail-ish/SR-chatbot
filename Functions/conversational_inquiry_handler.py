"""
Conversational Inquiry Handler for FG-Chatbot-03

Implements multi-turn conversation flow with state management.
"""

import logging
from typing import Optional
from io import BytesIO
from conversation_state import ConversationStateManager, ConversationStage
from template_account_statement_service import TemplateAccountStatementService

logger = logging.getLogger(__name__)


class ConversationalInquiryHandler:
    """Handles multi-turn conversational flow for inquiry processing."""
    
    def __init__(
        self,
        state_manager: ConversationStateManager,
        inquiry_manager,
        contract_service,
        account_statement_service,
        template_statement_service: TemplateAccountStatementService,
        wati_client,
        openai_client
    ):
        """
        Initialize conversational handler.
        
        Args:
            state_manager: ConversationStateManager instance
            inquiry_manager: InquiryManager instance
            contract_service: ContractService instance
            account_statement_service: AccountStatementService instance (legacy)
            template_statement_service: TemplateAccountStatementService instance
            wati_client: WATIClient instance
            openai_client: OpenAIClient instance
        """
        self.state_manager = state_manager
        self.inquiry_manager = inquiry_manager
        self.contract_service = contract_service
        self.account_statement_service = account_statement_service
        self.template_statement_service = template_statement_service
        self.wati_client = wati_client
        self.openai_client = openai_client
        
        logger.info("ConversationalInquiryHandler initialized")
    
    def process_message(self, message_text: str, sender: str) -> str:
        """
        Process user message based on conversation state.
        
        Args:
            message_text: User's message
            sender: WhatsApp number
        
        Returns:
            Response message
        """
        # Get current conversation state
        state = self.state_manager.get_state(sender)
        
        # Check for termination keywords
        if self._is_termination(message_text):
            self.state_manager.reset_state(sender)
            return "Session ended. Type 'Hi' to start a new inquiry."
        
        # Route based on current stage
        response = None

        # Handle disambiguation choice for name selection
        if state.stage == ConversationStage.NAME_VERIFICATION and state.awaiting_input == 'name_choice':
            response = self._handle_name_choice(message_text, state)
            self.state_manager.save_state(state)
            return response
        
        if state.stage == ConversationStage.GREETING:
            # If user greets, send welcome; otherwise treat input as a name and verify
            if self._is_greeting(message_text) or not message_text.strip():
                response = self._handle_greeting(message_text, state)
            else:
                response = self._handle_name_verification(message_text, state)
        
        elif state.stage == ConversationStage.NAME_VERIFICATION:
            response = self._handle_name_verification(message_text, state)
        
        elif state.stage == ConversationStage.DOCUMENT_CHOICE:
            response = self._handle_document_choice(message_text, state)
        
        elif state.stage == ConversationStage.ACCOUNT_STATEMENT_CHOICE:
            response = self._handle_account_statement_choice(message_text, state)
        
        elif state.stage == ConversationStage.CONTRACT_ID_INPUT:
            response = self._handle_contract_id_input(message_text, state)
        
        elif state.stage == ConversationStage.FOLLOW_UP:
            response = self._handle_follow_up(message_text, state)
        
        else:
            # Fallback - reset and start over
            response = self._handle_greeting(message_text, state)
        
        # Save state after processing
        self.state_manager.save_state(state)
        
        return response
    
    def _is_greeting(self, message: str) -> bool:
        """Check if message is a greeting."""
        greetings = ['hi', 'hello', 'hey', 'start', 'begin', 'restart']
        return any(greeting in message.lower().split() for greeting in greetings)
    
    def _is_start_over(self, message: str) -> bool:
        """Check if message is asking to start over (flexible matching)."""
        msg_lower = message.lower().strip()
        # Exact matches
        if msg_lower in ['start over', 'restart', 'start again', 'reset']:
            return True
        # Partial matches (handles typos like 'start ove')
        if 'start' in msg_lower and ('over' in msg_lower or 'ove' in msg_lower or 'again' in msg_lower):
            return True
        if 'restart' in msg_lower or 'reset' in msg_lower:
            return True
        return False
    
    def _is_termination(self, message: str) -> bool:
        """Check if message is a termination command."""
        terminations = ['no', 'end', 'stop', 'quit', 'exit', 'bye']
        msg_lower = message.lower().strip()
        return msg_lower in terminations or 'no thank' in msg_lower
    
    def _extract_name_with_ai(self, message: str) -> Optional[str]:
        """Use AI to extract name from message."""
        try:
            system_prompt = {
                "role": "system",
                "content": "Extract the person or company name from the message. Return only the name, nothing else. If no name found, return 'NONE'."
            }
            user_prompt = {
                "role": "user",
                "content": f"Message: {message}"
            }
            
            result = self.openai_client.chat_completion(
                messages=[system_prompt, user_prompt],
                temperature=0.3,
                max_tokens=50
            )
            
            name = result.get('content', '').strip()
            return name if name and name != 'NONE' else None
            
        except Exception as e:
            logger.error(f"Error extracting name: {e}")
            return None
    
    def _handle_greeting(self, message: str, state) -> str:
        """Handle greeting stage - ask for name."""
        state.stage = ConversationStage.NAME_VERIFICATION
        state.verification_attempts = 0
        return "Welcome to Smart Rental Inquiry Service!\n\nPlease provide the customer's name or company name for verification."
    
    def _handle_name_verification(self, message: str, state) -> str:
        """
        Handle name verification - check against Contract Report sheet.

        Searches both Customer Name and Company Name columns in Contract Report sheet
        to verify the user's identity against existing records.
        """
        # Check if user wants to start over
        if self._is_start_over(message):
            self.state_manager.reset_state(state.sender)
            state.stage = ConversationStage.GREETING
            return "Session restarted. Type 'Hi' to begin a new inquiry."

        # Extract name from message
        name = self._extract_name_with_ai(message)
        # Fallback: use raw message cleaned if AI couldn't extract
        if not name:
            import re
            raw = re.sub(r"\s+", " ", message).strip()
            # remove leading/trailing punctuation
            raw = re.sub(r"^[^\w]+|[^\w]+$", "", raw)
            # if it's a short single-word token (>=3 chars), try it
            tokens = raw.split()
            if len(tokens) == 1 and len(tokens[0]) >= 3:
                name = tokens[0]
            elif len(tokens) > 1:
                # use the whole cleaned phrase for broader matching
                name = raw
        if not name:
            return "I couldn't identify a name. Please provide the customer's company name or customer name."

        # Verify name against Contract Report sheet (searches both Customer Name and Company Name columns)
        contracts = self.contract_service.search_contracts(name)

        # Rank matches: exact phrase > startswith > contains
        def score_match(contract_row):
            company = str(contract_row.get('company name', '')).strip().lower()
            customer = str(contract_row.get('customer name', '')).strip().lower()
            query = name.strip().lower()
            # Ignore generic words like 'contract' when present in query
            query_clean = query.replace('contract', '').strip()
            scores = []
            for candidate in (company, customer):
                if candidate == query_clean:
                    scores.append(3)
                elif candidate.startswith(query_clean):
                    scores.append(2)
                elif query_clean in candidate:
                    scores.append(1)
                else:
                    scores.append(0)
            return max(scores)

        if contracts:
            ranked = sorted(contracts, key=score_match, reverse=True)
            top_score = score_match(ranked[0])
            # Filter to only reasonably matching entries (score >=1)
            filtered = [c for c in ranked if score_match(c) >= max(1, top_score)]

            if len(filtered) == 1:
                chosen = filtered[0]
                # Prefer company name if available
                chosen_name = chosen.get('company name') or chosen.get('customer name') or name
                state.user_name = chosen_name
                state.verified = True
                state.verification_attempts = 0
                state.stage = ConversationStage.DOCUMENT_CHOICE
                return f"Found: {chosen_name}\n\nWhat document do you need?\n- Contract Report\n- Account Statement"
            else:
                # Ask user to choose from available options (no limit)
                options = filtered
                raw_names = [
                    (opt.get('company name') or opt.get('customer name') or '').strip()
                    for opt in options
                ]
                # Normalize and deduplicate while preserving order
                deduped = []
                seen_keys = set()
                import re
                for name in raw_names:
                    if not name:
                        continue
                    # normalization key: lowercase + collapse internal whitespace
                    norm_key = " ".join(name.lower().split())
                    # filter out placeholders like '-' or names that reduce to empty after stripping punctuation
                    stripped_alnum = re.sub(r"[^a-z0-9]", "", norm_key)
                    if norm_key in {"-", "—", "n/a", "na"} or stripped_alnum == "":
                        continue
                    if norm_key in seen_keys:
                        continue
                    seen_keys.add(norm_key)
                    deduped.append(name)
                # Present all unique options (do not limit the number)
                state.pending_name_options = deduped
                if not state.pending_name_options:
                    # Fallback: treat original name as chosen
                    state.user_name = name
                    state.verified = True
                    state.stage = ConversationStage.DOCUMENT_CHOICE
                    return f"Found: {name}\n\nWhat document do you need?\n- Contract Report\n- Account Statement"
                # If only one unique option remains, auto-select it
                if len(state.pending_name_options) == 1:
                    chosen_name = state.pending_name_options[0]
                    state.user_name = chosen_name
                    state.verified = True
                    state.verification_attempts = 0
                    state.awaiting_input = None
                    state.pending_name_options = []
                    state.stage = ConversationStage.DOCUMENT_CHOICE
                    return f"Found: {chosen_name}\n\nWhat document do you need?\n- Contract Report\n- Account Statement"
                state.awaiting_input = 'name_choice'
                choices_text = "\n".join([f"{i+1}. {n}" for i, n in enumerate(state.pending_name_options)])
                return f"I found multiple matches ({len(state.pending_name_options)}). Please reply with a number:\n{choices_text}"
        else:
            # Name not found in Contract Report sheet
            state.verification_attempts += 1

            if state.verification_attempts >= 2:
                # Max attempts reached
                self.state_manager.reset_state(state.sender)
                return "Customer name not found in records after multiple attempts. Session ended. Type 'Hi' to start over."
            else:
                # Try again
                return "Customer name not found. Please check the spelling and try again, or type 'Start Over'."

    def _handle_name_choice(self, message: str, state) -> str:
        """Handle user selection from disambiguation list."""
        choice_text = message.strip()
        if not choice_text.isdigit():
            return "Please reply with a valid number for the chosen name."
        idx = int(choice_text) - 1
        if idx < 0 or idx >= len(state.pending_name_options):
            return "Number out of range. Please pick one of the listed options."
        chosen_name = state.pending_name_options[idx]
        state.user_name = chosen_name
        state.verified = True
        state.verification_attempts = 0
        state.awaiting_input = None
        state.pending_name_options = []
        state.stage = ConversationStage.DOCUMENT_CHOICE
        return f"Found: {chosen_name}\n\nWhat document do you need?\n- Contract Report\n- Account Statement"
    
    def _handle_document_choice(self, message: str, state) -> str:
        """Handle document type choice."""
        msg_lower = message.lower()
        
        # Check for contract report
        if 'contract' in msg_lower and 'report' in msg_lower:
            state.last_document_type = 'contract_report'
            state.stage = ConversationStage.FOLLOW_UP
            
            # Generate contract report
            return self._generate_contract_report(state.user_name, state.sender)
        
        # Check for account statement
        elif 'account' in msg_lower or 'statement' in msg_lower or 'soa' in msg_lower:
            state.last_document_type = 'account_statement'
            state.stage = ConversationStage.ACCOUNT_STATEMENT_CHOICE
            return "Account Statement for:\n- All Contracts\n- One Contract\n\nWhich one?"
        
        else:
            # Unclear choice
            return "Please specify which document:\n- Contract Report\n- Account Statement"
    
    def _handle_account_statement_choice(self, message: str, state) -> str:
        """Handle account statement choice (all or one) using AI."""
        msg_lower = message.lower()
        
        # Check for termination/cancellation first
        if self._is_termination(message) or 'cancel' in msg_lower or 'none' in msg_lower or 'nothing' in msg_lower:
            state.stage = ConversationStage.FOLLOW_UP
            return "Cancelled. Need anything else?\n- Contract Report\n- Account Statement\n- End"
        
        # Use AI to determine intent
        choice = self._extract_statement_choice_with_ai(message)
        
        if choice == 'all':
            # All contracts
            state.stage = ConversationStage.FOLLOW_UP
            return self._generate_all_account_statements(state.user_name, state.sender)
        
        elif choice == 'one':
            # One contract - ask for ID
            state.stage = ConversationStage.CONTRACT_ID_INPUT
            state.awaiting_input = 'contract_id'
            return "Enter the Contract ID:"
        
        else:
            # Unclear choice
            return "Please choose:\n- All Contracts\n- One Contract"
    
    def _extract_statement_choice_with_ai(self, message: str) -> Optional[str]:
        """Use AI to extract account statement choice (all or one)."""
        try:
            system_prompt = {
                "role": "system",
                "content": """Determine if the user wants:
- 'all' - all contracts/statements (keywords: all, everything, multiple, every)
- 'one' - one specific contract (keywords: one, single, specific, 1)

Return only 'all', 'one', or 'unclear'. Nothing else."""
            }
            user_prompt = {
                "role": "user",
                "content": f"User message: {message}"
            }
            
            result = self.openai_client.chat_completion(
                messages=[system_prompt, user_prompt],
                temperature=0.1,
                max_tokens=10
            )
            
            choice = result.get('content', '').strip().lower()
            
            if choice in ['all', 'one']:
                return choice
            return None
            
        except Exception as e:
            logger.error(f"Error extracting statement choice: {e}")
            return None
    
    def _handle_contract_id_input(self, message: str, state) -> str:
        """Handle contract ID input for single account statement."""
        # Check if user wants to start over
        if self._is_start_over(message):
            self.state_manager.reset_state(state.sender)
            state.stage = ConversationStage.GREETING
            state.verification_attempts = 0
            return "Session restarted. Type 'Hi' to begin a new inquiry."

        # Extract potential contract ID
        contract_id = message.strip()

        # Verify contract ID exists
        summary_records = self.account_statement_service.search_account_summary(contract_id)
        detail_records = self.account_statement_service.search_account_details(contract_id)

        if summary_records or detail_records:
            # Valid contract ID
            state.stage = ConversationStage.FOLLOW_UP
            state.awaiting_input = None
            state.verification_attempts = 0  # Reset attempts counter
            return self._generate_single_account_statement(contract_id, state.sender)
        else:
            # Invalid contract ID
            state.verification_attempts += 1

            if state.verification_attempts >= 2:
                # Reset the state object itself before saving
                state.stage = ConversationStage.GREETING
                state.verification_attempts = 0
                state.user_name = None
                state.verified = False
                state.last_document_type = None
                state.awaiting_input = None
                self.state_manager.reset_state(state.sender)
                return "Contract ID not found after multiple attempts. Session ended. Type 'Hi' to start over."
            else:
                return "Contract ID not found. Please check and re-enter, or type 'Start Over'."
    
    def _handle_follow_up(self, message: str, state) -> str:
        """Handle follow-up after document delivery."""
        msg_lower = message.lower()
        
        # Check for contract report request
        if 'contract' in msg_lower and 'report' in msg_lower:
            state.last_document_type = 'contract_report'
            return self._generate_contract_report(state.user_name, state.sender)
        
        # Check for account statement request
        elif 'account' in msg_lower or 'statement' in msg_lower:
            state.last_document_type = 'account_statement'
            state.stage = ConversationStage.ACCOUNT_STATEMENT_CHOICE
            return "Account Statement for:\n- All Contracts\n- One Contract\n\nWhich one?"
        
        # Check for termination
        elif self._is_termination(message):
            self.state_manager.reset_state(state.sender)
            return "Session ended. Type 'Hi' to start a new inquiry."
        
        else:
            return "Need anything else?\n- Contract Report\n- Account Statement\n- End"
    
    def _generate_contract_report(self, customer_name: str, sender: str) -> str:
        """Generate and send contract report using existing service."""
        return self.inquiry_manager._handle_contract_report(customer_name, sender)
    
    def _generate_single_account_statement(self, contract_id: str, sender: str) -> str:
        """Generate and send single account statement using template service."""
        try:
            logger.info(f"Generating single account statement for {contract_id}")
            
            # Generate PDF using template service
            pdf_url = self.template_statement_service.generate_single_statement(contract_id)
            
            if not pdf_url:
                return "❌ Error generating account statement. Please try again."
            
            # Send PDF link
            message = f"✅ Account statement for {contract_id} is ready!\n\n{pdf_url}\n\nNeed anything else?"
            return message
            
        except Exception as e:
            logger.error(f"Error in single statement generation: {e}", exc_info=True)
            return "❌ Error generating account statement."
    
    def _generate_all_account_statements(self, customer_name: str, sender: str) -> str:
        """Generate and send all account statements for a customer using template service."""
        try:
            logger.info(f"Generating all account statements for {customer_name}")
            
            # Get all contract IDs for this customer
            contracts = self.contract_service.search_contracts(customer_name)
            
            if not contracts:
                return f"No contracts found for {customer_name}."
            
            # Extract contract IDs
            contract_ids = [c.get('contract id') for c in contracts if c.get('contract id')]
            
            if not contract_ids:
                return f"No valid contract IDs found for {customer_name}."
            
            logger.info(f"Found {len(contract_ids)} contracts: {contract_ids}")
            
            # Generate PDF using template service
            pdf_url = self.template_statement_service.generate_multi_statement(contract_ids)
            
            if not pdf_url:
                return "❌ Error generating account statements. Please try again."
            
            # Send PDF link
            message = f"✅ Account statements for {customer_name} are ready!\n\nContracts: {len(contract_ids)}\n\n{pdf_url}\n\nNeed anything else? Contract Report, Account Statement, or End?"
            return message
            
        except Exception as e:
            logger.error(f"Error in multi statement generation: {e}", exc_info=True)
            return "❌ Error generating account statements."
