import os
import json
import logging
import sys
import pathlib

# Ensure chatbot_core is importable
core_layer_path = pathlib.Path(__file__).parent / "chatbot_core_layer" / "python"
if not core_layer_path.exists():
    core_layer_path = pathlib.Path(__file__).parent / "chatbot-core-layer" / "python"
if str(core_layer_path) not in sys.path:
    sys.path.insert(0, str(core_layer_path))

# Import core clients
from chatbot_core import (
    ConfigManager, WATIClient, OpenAIClient, GoogleSheetsClient, GoogleDriveClient,
    ConversationManager, DatabaseClient
)

# Import inquiry bot extension
from chatbot_core.extensions.inquiry_bot import InquiryManager

# Import conversational handler
from conversation_state import ConversationStateManager
from conversational_inquiry_handler import ConversationalInquiryHandler
from database_extensions import DatabaseClientExtensions
from template_account_statement_service import TemplateAccountStatementService

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load config
CONFIG_FILE = os.environ.get("CONFIG_FILE", "config.json")
config_manager = ConfigManager(CONFIG_FILE)

# Initialize clients
wati_client = WATIClient(CONFIG_FILE)
openai_client = OpenAIClient(CONFIG_FILE)
sheets_client = GoogleSheetsClient(CONFIG_FILE)
drive_client = GoogleDriveClient(CONFIG_FILE)
conversation_manager = ConversationManager(openai_client, config_manager)

# Initialize database client (optional - continues without it)
try:
    db_client = DatabaseClient(CONFIG_FILE)
    # Extend database client with state management methods
    DatabaseClientExtensions.add_state_methods(db_client)
    logger.info("DatabaseClient initialized successfully")
except Exception as e:
    logger.warning(f"DatabaseClient initialization failed: {e}. Continuing without database logging.")
    db_client = None

# ============================================================================
# CUSTOMIZABLE SECTION - Edit these to modify AI behavior
# ============================================================================

# System prompt for parsing customer inquiries
INQUIRY_PARSER_SYSTEM_PROMPT = """
You are a data extraction assistant for a Smart Rental Inquiry system.

Your task is to extract structured information from customer messages about rental inquiries.

EXTRACTION RULES:
1. search_name: Extract any company or person name mentioned
   - Could be company name (ABC Corp, Tech Solutions, etc.)
   - Could be person name (John Smith, Sarah Lee, etc.)
   - Extract whatever name is provided, don't worry about type

2. info_type: Identify what information they want:
   - 'invoice_details': Keywords like 'invoice', 'billing', 'bill'
   - 'account_statement': Keywords like 'account', 'statement', 'balance'
   - 'contract_report': Keywords like 'contract', 'agreement', 'rental contract', 'lease'

3. contract_id: Extract Contract ID if info_type is 'account_statement'
   - Look for contract numbers, IDs, codes (e.g., SR251206001, PI-SO-01135)
   - Usually alphanumeric with dashes or numbers

4. confidence: Rate your confidence in the extraction (0-1)
   - 0.9-1.0: All information clearly stated
   - 0.7-0.8: Most information clear, some ambiguity
   - 0.5-0.6: Significant ambiguity or missing info
   - Below 0.5: Cannot extract reliably

EXAMPLES:
Input: 'Hi, I'm John from ABC Corp. We need contract reports for our rentals.'
Output: {
  'search_name': 'ABC Corp',
  'info_type': 'contract_report',
  'confidence': 0.95
}

Input: 'Hello, my name is Sarah Lee. Can I get my contract report?'
Output: {
  'search_name': 'Sarah Lee',
  'info_type': 'contract_report',
  'confidence': 0.9
}

Input: 'Need account statement for contract SR250129004'
Output: {
  'info_type': 'account_statement',
  'contract_id': 'SR250129004',
  'confidence': 0.95
}

Return only valid JSON with the specified fields.
"""

# OpenAI function definition for parsing inquiries
PARSE_INQUIRY_FUNCTION = {
    "name": "parseRentalInquiry",
    "description": "Parse customer's rental inquiry message to extract name and information type requested.",
    "parameters": {
        "type": "object",
        "properties": {
            "search_name": {
                "type": "string",
                "description": "Company name or person name mentioned in the message"
            },
            "info_type": {
                "type": "string",
                "enum": ["invoice_details", "account_statement", "contract_report"],
                "description": "Type of information requested: invoice_details, account_statement, or contract_report"
            },
            "contract_id": {
                "type": "string",
                "description": "Contract ID (required if info_type is 'account_statement')"
            },
            "confidence": {
                "type": "number",
                "description": "Confidence score 0-1 for the extraction"
            }
        },
        "required": ["info_type", "confidence"]
    }
}

# ============================================================================
# END CUSTOMIZABLE SECTION
# ============================================================================

# Initialize Inquiry Manager
inquiry_manager = InquiryManager(
    openai_client=openai_client,
    sheets_client=sheets_client,
    wati_client=wati_client,
    conversation_manager=conversation_manager,
    db_client=db_client,
    config_path=CONFIG_FILE,
    system_prompt=INQUIRY_PARSER_SYSTEM_PROMPT,
    function_definition=PARSE_INQUIRY_FUNCTION
)

# Initialize Conversation State Manager
state_manager = ConversationStateManager(db_client=db_client)

# Initialize Template Account Statement Service
template_statement_service = TemplateAccountStatementService(
    sheets_client=sheets_client,
    drive_client=drive_client
)

# Initialize Conversational Handler
conversational_handler = ConversationalInquiryHandler(
    state_manager=state_manager,
    inquiry_manager=inquiry_manager,
    contract_service=inquiry_manager.contract_service,
    account_statement_service=inquiry_manager.account_statement_service,
    template_statement_service=template_statement_service,
    wati_client=wati_client,
    openai_client=openai_client
)





def lambda_handler(event, context):
    """AWS Lambda handler for Smart Rental Inquiry Chatbot."""
    try:
        logger.info(f"Raw event received: {json.dumps(event)}")
        
        if not event:
            logger.error("Received empty event")
            return {'statusCode': 400, 'body': json.dumps({'error': 'Empty event'})}
        
        # Extract message data from event
        message_data = None
        
        # Check if this is an EventBridge event
        if 'detail' in event:
            logger.info("Processing EventBridge event")
            detail = event['detail']
            message_data = {
                'sender': detail.get('sender'),
                'text': detail.get('incoming_msg', ''),
                'is_twilio': detail.get('is_twilio', False)
            }
        else:
            # API Gateway or direct invocation
            body = event.get('body') if isinstance(event, dict) else event
            logger.info(f"Body after initial extraction: {body}")
            
            if isinstance(body, str):
                body = json.loads(body)
            
            if not body:
                logger.error("Received empty body after parsing")
                return {'statusCode': 400, 'body': json.dumps({'error': 'Empty body'})}
            
            # Extract from WATI webhook format
            if 'detail' in body:
                detail = body['detail']
                message_data = {
                    'sender': detail.get('sender'),
                    'text': detail.get('incoming_msg', ''),
                    'is_twilio': detail.get('is_twilio', False)
                }
            else:
                sender = body.get('sender') or (body.get('data', {}) or {}).get('sender')
                text = (body.get('data', {}) or {}).get('text') or body.get('text') or ''
                message_data = {
                    'sender': sender,
                    'text': text,
                    'is_twilio': False
                }
        
        if not message_data or not message_data.get('sender'):
            logger.error("Could not extract sender from event")
            return {'statusCode': 400, 'body': json.dumps({'error': 'Invalid message format'})}
        
        sender = message_data['sender']
        text = message_data.get('text', '').strip()
        
        if not text:
            logger.warning(f"Empty message text from {sender}")
            return {'statusCode': 200, 'body': json.dumps({'status': 'ignored_empty'})}
        
        logger.info(f"Processing message from {sender}: {text[:100]}")
        
        # Save user message to database
        if db_client:
            try:
                db_client.save_message(sender, 'user', text)
            except Exception as e:
                logger.warning(f"Failed to save user message to DB: {e}")
        
        # Process the inquiry using Conversational Handler
        response_text = conversational_handler.process_message(text, sender)
        
        # Save assistant response to database
        if db_client:
            try:
                db_client.save_message(sender, 'assistant', response_text)
            except Exception as e:
                logger.warning(f"Failed to save assistant message to DB: {e}")
        
        # Send response
        wati_client.send_message(response_text, sender)
        logger.info(f"Response sent to {sender}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'message': 'Inquiry processed successfully'
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda handler error: {e}", exc_info=True)
        
        # Try to send error message to user
        try:
            if message_data and message_data.get('sender'):
                wati_client.send_message(
                    "Sorry, something went wrong. Please try again later or contact support.",
                    message_data['sender']
                )
        except Exception:
            pass
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
