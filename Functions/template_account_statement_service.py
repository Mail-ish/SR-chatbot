"""
Template-Based Account Statement Service

Generates account statements using pre-formatted Excel templates.
Supports both single and multiple contract ID statements.
Uses Google Drive to create working copies, fill data, export as PDF, and share.
"""

import logging
import re
from io import BytesIO
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class TemplateAccountStatementService:
    """Service for generating template-based account statements."""
    
    # Template spreadsheet configuration
    TEMPLATE_SHEET_ID = "1anXW6cxvMGA066b9t53fHe6ify2F37uy_6UmBrnrpv4"
    MULTI_TEMPLATE_SHEET = "Multi"
    SINGLE_TEMPLATE_SHEET = "Single"
    
    # Working folder for temporary copies
    WORKING_FOLDER_ID = "104lrYw0k_ohnPCFCpFGhnBktSekP_8MN"
    
    # Account Statement data source
    ACCOUNT_STATEMENT_SHEET_ID = "1dk-iP5a0iSbXzdNN0ZF_9uCHfSFVUMVVONX0w1xN_yw"
    ACCOUNT_SUMMARY_SHEET = "Account Statement - summarised"
    
    # Contract Report data source
    CONTRACT_REPORT_SHEET_ID = "17kaq3n07ZUknm2OgpvMfoaoXU3tuuRxQCC1ChwHDlEk"
    CONTRACT_REPORT_SHEET = "Contract Report"
    
    def __init__(self, sheets_client, drive_client, openai_client=None):
        """
        Initialize Template Account Statement Service.

        Args:
            sheets_client: GoogleSheetsClient instance
            drive_client: GoogleDriveClient instance
            openai_client: Optional OpenAIClient instance for intelligent address parsing
        """
        self.sheets_client = sheets_client
        self.drive_client = drive_client
        self.openai_client = openai_client
        logger.info("TemplateAccountStatementService initialized")
    
    def generate_single_statement(self, contract_id: str) -> Optional[str]:
        """
        Generate account statement for a single contract using template.
        
        Args:
            contract_id: Contract ID to generate statement for
        
        Returns:
            PDF URL or None if failed
        """
        try:
            logger.info(f"Generating single contract statement for: {contract_id}")
            
            # Collect data
            contract_data = self.get_contract_data([contract_id])
            if not contract_data:
                logger.error(f"No contract data found for {contract_id}")
                return None
            
            contract_info = contract_data[0]
            
            summary_data = self.get_account_summary_data([contract_id])
            detail_data = self.get_account_detail_data([contract_id])
            
            # Get planet points (try customer name first, then company name)
            user_name = contract_info.get('customer_name') or contract_info.get('company_name', '')
            planet_points = self.get_planet_points(user_name)
            
            # Create working copy of template
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            working_copy_name = f"Statement_Single_{contract_id}_{timestamp}"
            
            working_copy_id = self.drive_client.copy_file(
                source_file_id=self.TEMPLATE_SHEET_ID,
                destination_folder_id=self.WORKING_FOLDER_ID,
                new_file_name=working_copy_name
            )
            
            if not working_copy_id:
                logger.error("Failed to create working copy")
                return None
            
            logger.info(f"Created working copy: {working_copy_id}")
            
            # Delete unused Multi sheet tab
            self.delete_sheet_tab(working_copy_id, self.MULTI_TEMPLATE_SHEET)
            
            # Fill template with data
            self.fill_single_template(
                working_copy_id,
                contract_info,
                summary_data,
                detail_data,
                planet_points
            )

            # Export as PDF bytes (with gridlines hidden)
            pdf_bytes = self.export_sheet_as_pdf(working_copy_id, self.SINGLE_TEMPLATE_SHEET)
            
            if not pdf_bytes:
                logger.error("Failed to export PDF")
                self.drive_client.delete_file(working_copy_id)
                return None
            
            # Upload PDF to Drive
            pdf_filename = f"Statement_Single_{contract_id}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.pdf"
            pdf_file_id = self.drive_client.upload_file(
                file_data=pdf_bytes,
                filename=pdf_filename,
                folder_id=self.WORKING_FOLDER_ID,
                mime_type='application/pdf'
            )
            
            # Delete working copy spreadsheet
            self.cleanup_working_copy(working_copy_id)
            
            if not pdf_file_id:
                logger.error("Failed to upload PDF")
                return None
            
            # Get shareable link
            pdf_url = self.drive_client.get_file_link(pdf_file_id)
            
            if pdf_url:
                logger.info(f"Single statement PDF generated: {pdf_url}")
            
            return pdf_url
            
        except Exception as e:
            logger.error(f"Error generating single statement: {e}", exc_info=True)
            return None
    
    def generate_multi_statement(self, contract_ids: List[str]) -> Optional[str]:
        """
        Generate account statement for multiple contracts using template.
        
        Args:
            contract_ids: List of Contract IDs
        
        Returns:
            PDF URL or None if failed
        """
        try:
            logger.info(f"Generating multi-contract statement for: {contract_ids}")
            
            # Collect data
            contracts_data = self.get_contract_data(contract_ids)
            if not contracts_data:
                logger.error("No contract data found")
                return None
            
            summary_data = self.get_account_summary_data(contract_ids)
            details_by_contract = {}
            for cid in contract_ids:
                details_by_contract[cid] = self.get_account_detail_data([cid])
            
            # Get planet points using first contract's customer name
            user_name = contracts_data[0].get('customer_name') or contracts_data[0].get('company_name', '')
            planet_points = self.get_planet_points(user_name)
            
            # Create working copy of template
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            customer_name_safe = (user_name[:20].replace(' ', '_').replace('/', '_'))
            working_copy_name = f"Statement_Multi_{customer_name_safe}_{timestamp}"
            
            working_copy_id = self.drive_client.copy_file(
                source_file_id=self.TEMPLATE_SHEET_ID,
                destination_folder_id=self.WORKING_FOLDER_ID,
                new_file_name=working_copy_name
            )
            
            if not working_copy_id:
                logger.error("Failed to create working copy")
                return None
            
            logger.info(f"Created working copy: {working_copy_id}")
            
            # Delete unused Single sheet tab
            self.delete_sheet_tab(working_copy_id, self.SINGLE_TEMPLATE_SHEET)
            
            # Fill template with data
            self.fill_multi_template(
                working_copy_id,
                contracts_data,
                summary_data,
                details_by_contract,
                planet_points
            )

            # Export as PDF bytes (with gridlines hidden)
            pdf_bytes = self.export_sheet_as_pdf(working_copy_id, self.MULTI_TEMPLATE_SHEET)
            
            if not pdf_bytes:
                logger.error("Failed to export PDF")
                self.drive_client.delete_file(working_copy_id)
                return None
            
            # Upload PDF to Drive
            pdf_filename = f"Statement_Multi_{customer_name_safe}_{timestamp}.pdf"
            pdf_file_id = self.drive_client.upload_file(
                file_data=pdf_bytes,
                filename=pdf_filename,
                folder_id=self.WORKING_FOLDER_ID,
                mime_type='application/pdf'
            )
            
            # Delete working copy spreadsheet
            self.cleanup_working_copy(working_copy_id)
            
            if not pdf_file_id:
                logger.error("Failed to upload PDF")
                return None
            
            # Get shareable link
            pdf_url = self.drive_client.get_file_link(pdf_file_id)
            
            if pdf_url:
                logger.info(f"Multi statement PDF generated: {pdf_url}")
            
            return pdf_url
            
        except Exception as e:
            logger.error(f"Error generating multi statement: {e}", exc_info=True)
            return None
    
    def get_contract_data(self, contract_ids: List[str]) -> List[Dict]:
        """
        Get contract information from Contract Report sheet.
        
        Args:
            contract_ids: List of contract IDs to fetch
        
        Returns:
            List of contract dictionaries
        """
        try:
            logger.info(f"Fetching contract data for: {contract_ids}")
            
            data = self.sheets_client.read_range(
                f"{self.CONTRACT_REPORT_SHEET}!A:M",
                sheet_id=self.CONTRACT_REPORT_SHEET_ID,
                use_cache=False
            )
            
            if not data or len(data) < 2:
                return []
            
            headers = data[0]
            header_map = {h.strip().lower(): idx for idx, h in enumerate(headers)}
            
            # Column indices
            contract_id_idx = header_map.get("contract id")
            company_name_idx = header_map.get("company name")
            customer_name_idx = header_map.get("customer name")
            delivery_address_idx = header_map.get("delivery address")
            customer_code_idx = header_map.get("customer code")
            start_date_idx = header_map.get("start date")
            end_date_idx = header_map.get("end date")
            email_idx = header_map.get("email")
            
            contracts = []
            contract_ids_lower = [cid.strip().lower() for cid in contract_ids]
            
            for row in data[1:]:
                if contract_id_idx and len(row) > contract_id_idx:
                    row_contract_id = str(row[contract_id_idx]).strip().lower()
                    
                    if row_contract_id in contract_ids_lower:
                        contract = {
                            'contract_id': row[contract_id_idx] if contract_id_idx and len(row) > contract_id_idx else '',
                            'company_name': row[company_name_idx] if company_name_idx and len(row) > company_name_idx else '',
                            'customer_name': row[customer_name_idx] if customer_name_idx and len(row) > customer_name_idx else '',
                            'delivery_address': row[delivery_address_idx] if delivery_address_idx and len(row) > delivery_address_idx else '',
                            'customer_code': row[customer_code_idx] if customer_code_idx and len(row) > customer_code_idx else '',
                            'start_date': row[start_date_idx] if start_date_idx and len(row) > start_date_idx else '',
                            'end_date': row[end_date_idx] if end_date_idx and len(row) > end_date_idx else '',
                            'email': row[email_idx] if email_idx and len(row) > email_idx else ''
                        }
                        contracts.append(contract)
            
            logger.info(f"Found {len(contracts)} contracts")
            return contracts
            
        except Exception as e:
            logger.error(f"Error fetching contract data: {e}", exc_info=True)
            return []
    
    def get_account_summary_data(self, contract_ids: List[str]) -> Dict:
        """
        Get account summary data and calculate totals.
        
        Args:
            contract_ids: List of contract IDs
        
        Returns:
            Dictionary with total_invoiced, total_paid, outstanding
        """
        try:
            logger.info(f"Fetching account summary for: {contract_ids}")
            
            data = self.sheets_client.read_range(
                f"{self.ACCOUNT_SUMMARY_SHEET}!A:J",
                sheet_id=self.ACCOUNT_STATEMENT_SHEET_ID,
                use_cache=False
            )
            
            if not data or len(data) < 2:
                return {'total_invoiced': 0, 'total_paid': 0, 'outstanding': 0}
            
            headers = data[0]
            header_map = {h.strip().lower(): idx for idx, h in enumerate(headers)}
            
            contract_id_idx = header_map.get("contract id")
            total_invoiced_idx = header_map.get("total invoiced")
            total_paid_idx = header_map.get("total paid")
            outstanding_idx = header_map.get("outstanding")
            
            total_invoiced = 0
            total_paid = 0
            outstanding = 0
            
            contract_ids_lower = [cid.strip().lower() for cid in contract_ids]
            
            for row in data[1:]:
                if contract_id_idx and len(row) > contract_id_idx:
                    row_contract_id = str(row[contract_id_idx]).strip().lower()
                    
                    if row_contract_id in contract_ids_lower:
                        try:
                            if total_invoiced_idx and len(row) > total_invoiced_idx:
                                total_invoiced += float(row[total_invoiced_idx] or 0)
                        except (ValueError, TypeError):
                            pass
                        
                        try:
                            if total_paid_idx and len(row) > total_paid_idx:
                                total_paid += float(row[total_paid_idx] or 0)
                        except (ValueError, TypeError):
                            pass
                        
                        try:
                            if outstanding_idx and len(row) > outstanding_idx:
                                outstanding += float(row[outstanding_idx] or 0)
                        except (ValueError, TypeError):
                            pass
            
            logger.info(f"Summary totals: invoiced={total_invoiced}, paid={total_paid}, outstanding={outstanding}")
            
            return {
                'total_invoiced': total_invoiced,
                'total_paid': total_paid,
                'outstanding': outstanding
            }
            
        except Exception as e:
            logger.error(f"Error fetching summary data: {e}", exc_info=True)
            return {'total_invoiced': 0, 'total_paid': 0, 'outstanding': 0}
    
    def get_account_detail_data(self, contract_ids: List[str]) -> List[Dict]:
        """
        Get invoice detail data from Account Statement sheets.
        
        Args:
            contract_ids: List of contract IDs
        
        Returns:
            List of invoice detail dictionaries
        """
        try:
            logger.info(f"Fetching account details for: {contract_ids}")
            
            all_details = []
            sheet_names = [
                "Account Statement",
                "Account Statement (2)",
                "Account Statement (3)",
                "Account Statement (4)",
                "Account Statement (5)"
            ]
            
            contract_ids_lower = [cid.strip().lower() for cid in contract_ids]
            
            for sheet_name in sheet_names:
                try:
                    data = self.sheets_client.read_range(
                        f"{sheet_name}!A:K",
                        sheet_id=self.ACCOUNT_STATEMENT_SHEET_ID,
                        use_cache=False
                    )
                    
                    if not data or len(data) < 2:
                        continue
                    
                    headers = data[0]
                    header_map = {h.strip().lower(): idx for idx, h in enumerate(headers)}
                    
                    contract_id_idx = header_map.get("contract id")
                    
                    for row in data[1:]:
                        if contract_id_idx and len(row) > contract_id_idx:
                            row_contract_id = str(row[contract_id_idx]).strip().lower()
                            
                            if row_contract_id in contract_ids_lower:
                                detail = {}
                                for header, idx in header_map.items():
                                    detail[header] = row[idx] if idx < len(row) else ''
                                all_details.append(detail)
                    
                except Exception as e:
                    logger.warning(f"Error reading {sheet_name}: {e}")
                    continue
            
            logger.info(f"Found {len(all_details)} detail records")
            return all_details
            
        except Exception as e:
            logger.error(f"Error fetching detail data: {e}", exc_info=True)
            return []
    
    def get_planet_points(self, user_name: str) -> float:
        """
        Get total planet points for a user.
        
        Args:
            user_name: Customer/company name to match
        
        Returns:
            Total points (float)
        """
        try:
            logger.info(f"Fetching planet points for: {user_name}")
            
            # Try to read Planet Point sheet from the same spreadsheet as Account Statement
            data = self.sheets_client.read_range(
                "Planet Point!A:G",
                sheet_id=self.ACCOUNT_STATEMENT_SHEET_ID,
                use_cache=False
            )
            
            if not data or len(data) < 2:
                logger.warning("Planet Point sheet is empty or not found")
                return 0.0
            
            headers = data[0]
            header_map = {h.strip().lower(): idx for idx, h in enumerate(headers)}
            
            user_name_idx = header_map.get("user_name") or header_map.get("customer name")
            points_idx = header_map.get("points")
            
            if not user_name_idx or not points_idx:
                logger.warning("Required columns not found in Planet Point sheet")
                return 0.0
            
            total_points = 0.0
            user_name_lower = user_name.strip().lower()
            
            for row in data[1:]:
                if len(row) > user_name_idx:
                    row_user_name = str(row[user_name_idx]).strip().lower()
                    
                    if user_name_lower in row_user_name or row_user_name in user_name_lower:
                        try:
                            if len(row) > points_idx:
                                total_points += float(row[points_idx] or 0)
                        except (ValueError, TypeError):
                            pass
            
            logger.info(f"Total planet points: {total_points}")
            return total_points
            
        except Exception as e:
            logger.warning(f"Error fetching planet points: {e}")
            return 0.0
    
    def parse_delivery_address(self, address: str) -> Tuple[str, str, str]:
        """
        Parse delivery address into 3 lines with postcode at start of line 3.
        Uses OpenAI for intelligent parsing if available, otherwise falls back to regex.

        Args:
            address: Full delivery address string

        Returns:
            Tuple of (line1, line2, line3) where line3 starts with postcode
        """
        if not address:
            return ('', '', '')

        # Try OpenAI parsing first if available
        if self.openai_client:
            try:
                logger.info("Using OpenAI to parse delivery address")
                prompt = f"""Parse this delivery address into exactly 3 lines following these rules:
1. Line 1: Street address/building number and name
2. Line 2: Area/district/city/state and country (combine all remaining parts)
3. Line 3: Just the postal/zip code (5-6 digits)

Address: {address}

Return ONLY a JSON object with this exact format (no additional text):
{{"line1": "...", "line2": "...", "line3": "..."}}"""

                messages = [{"role": "user", "content": prompt}]
                response = self.openai_client.chat_completion(
                    messages=messages,
                    temperature=0.1,
                    max_tokens=200
                )

                content = response.get("content", "").strip()
                # Try to extract JSON from response
                import json
                # Find JSON object in response
                start_idx = content.find('{')
                end_idx = content.rfind('}') + 1
                if start_idx >= 0 and end_idx > start_idx:
                    json_str = content[start_idx:end_idx]
                    parsed = json.loads(json_str)
                    line1 = parsed.get("line1", "").strip()
                    line2 = parsed.get("line2", "").strip()
                    line3 = parsed.get("line3", "").strip()

                    if line1 or line2 or line3:
                        logger.info(f"OpenAI parsed address: L1={line1}, L2={line2}, L3={line3}")
                        return (line1, line2, line3)

            except Exception as e:
                logger.warning(f"OpenAI address parsing failed, falling back to regex: {e}")

        # Fallback to regex-based parsing
        logger.info("Using regex-based address parsing")

        # Postcode patterns (common formats)
        postcode_patterns = [
            r'\b\d{5}(?:-\d{4})?\b',  # US: 12345 or 12345-6789
            r'\b[A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2}\b',  # UK: SW1A 1AA
            r'\b\d{5}\b',  # Simple 5-digit
            r'\b\d{6}\b',  # 6-digit postcode
        ]

        # Try to find postcode
        postcode = ''
        postcode_match = None
        for pattern in postcode_patterns:
            match = re.search(pattern, address, re.IGNORECASE)
            if match:
                postcode = match.group().strip()
                postcode_match = match
                break

        # Remove postcode from address for splitting
        address_without_postcode = address
        if postcode_match:
            address_without_postcode = address[:postcode_match.start()] + address[postcode_match.end():]

        # Split by common delimiters
        parts = re.split(r'[,\n]+', address_without_postcode)
        parts = [p.strip() for p in parts if p.strip()]

        # Distribute into 3 lines
        if len(parts) == 0:
            line1 = ''
            line2 = ''
        elif len(parts) == 1:
            line1 = parts[0]
            line2 = ''
        elif len(parts) == 2:
            line1 = parts[0]
            line2 = parts[1]
        else:
            # More than 2 parts - combine all remaining parts into line2
            line1 = parts[0]
            line2 = ', '.join(parts[1:])

        # Line 3 starts with postcode
        line3 = postcode if postcode else ''

        return (line1, line2, line3)
    
    def fill_single_template(
        self,
        spreadsheet_id: str,
        contract_info: Dict,
        summary_data: Dict,
        detail_data: List[Dict],
        planet_points: float
    ):
        """
        Fill Single template with data using Google Sheets API.
        
        Args:
            spreadsheet_id: Working copy spreadsheet ID
            contract_info: Contract information dictionary
            summary_data: Summary totals dictionary
            detail_data: List of invoice details
            planet_points: Total planet points
        """
        try:
            # Prepare batch update requests
            updates = []
            
            # Customer name/company name (A10)
            customer_name = contract_info.get('customer_name') or contract_info.get('company_name', '')
            updates.append({
                'range': f'{self.SINGLE_TEMPLATE_SHEET}!A10',
                'values': [[customer_name]]
            })
            
            # Delivery address (A11, A12, A13)
            address = contract_info.get('delivery_address', '')
            line1, line2, line3 = self.parse_delivery_address(address)
            updates.extend([
                {'range': f'{self.SINGLE_TEMPLATE_SHEET}!A11', 'values': [[line1]]},
                {'range': f'{self.SINGLE_TEMPLATE_SHEET}!A12', 'values': [[line2]]},
                {'range': f'{self.SINGLE_TEMPLATE_SHEET}!A13', 'values': [[line3]]}
            ])

            # Customer email (A14)
            customer_email = f"EMAIL: {contract_info.get('email', '')}"
            updates.append({
                'range': f'{self.SINGLE_TEMPLATE_SHEET}!A14',
                'values': [[customer_email]]
            })
            
            # Customer code (I10)
            updates.append({
                'range': f'{self.SINGLE_TEMPLATE_SHEET}!I10',
                'values': [[contract_info.get('customer_code', '')]]
            })
            
            # Statement date (I11)
            updates.append({
                'range': f'{self.SINGLE_TEMPLATE_SHEET}!I11',
                'values': [[datetime.now(timezone.utc).strftime("%Y-%m-%d")]]
            })
            
            # Totals (I12, I13, I14)
            updates.extend([
                {'range': f'{self.SINGLE_TEMPLATE_SHEET}!I12', 'values': [[summary_data.get('total_invoiced', 0)]]},
                {'range': f'{self.SINGLE_TEMPLATE_SHEET}!I13', 'values': [[summary_data.get('total_paid', 0)]]},
                {'range': f'{self.SINGLE_TEMPLATE_SHEET}!I14', 'values': [[summary_data.get('outstanding', 0)]]}
            ])
            
            # Planet points (D26)
            earned_pp = f": {[planet_points]}"
            updates.append({
                'range': f'{self.SINGLE_TEMPLATE_SHEET}!D26',
                'values': [[earned_pp]]
            })
            
            # Contract header (A16)
            contract_header = f"CONTRACT #{contract_info.get('contract_id', '')} ({contract_info.get('start_date', '')} - {contract_info.get('end_date', '')})"
            updates.append({
                'range': f'{self.SINGLE_TEMPLATE_SHEET}!A16',
                'values': [[contract_header]]
            })
            
            # Batch update all cells (except invoice details)
            self.sheets_client.batch_update(updates, sheet_id=spreadsheet_id)

            ### Invoice details - insert rows after contract header row 16
            if detail_data:

                # --- NEW: normalize invoice/receipt dates so "2023-03" → "01/03/2023" ---
                def normalize_date(val):
                    if not val:
                        return ''
                    if isinstance(val, str) and len(val) == 7 and val.count('-') == 1:
                        # Convert YYYY-MM → 01/MM/YYYY
                        year, month = val.split('-')
                        return f"01/{month}/{year}"
                    return val

                # --- NEW: split receipts + invoices ---
                invoices = []
                receipts = []

                for d in detail_data:
                    entry = {
                        'invoice no.': d.get('invoice no.', ''),
                        'month': normalize_date(d.get('month')),
                        'invoiced amount': d.get('invoiced amount', ''),
                        'payment status': d.get('payment status', ''),
                        'paid at': normalize_date(d.get('paid at')),
                        'total paid': d.get('total paid', ''),
                        'outstanding amount': d.get('outstanding amount', ''),
                        'type': d.get('type', 'invoice')  # assume missing => invoice
                    }

                    if entry['type'] == 'receipt':
                        receipts.append(entry)
                    else:
                        invoices.append(entry)

                # --- NEW: sort each group by month/date ---
                def sort_key(x):
                    return x['month'] or ''

                invoices.sort(key=sort_key)
                receipts.sort(key=sort_key)

                # Merge in order → invoices first, then receipts
                sorted_details = invoices + receipts

                # Total rows needed: detail rows + 1 balance summary row
                num_rows_to_insert = len(sorted_details) + 1

                # Insert rows starting at row 17 (after contract header at row 16)
                self.insert_rows_with_formatting(
                    spreadsheet_id=spreadsheet_id,
                    sheet_name=self.SINGLE_TEMPLATE_SHEET,
                    start_row=17,
                    num_rows=num_rows_to_insert,
                    source_row=15  # Copy formatting from header row 15
                )

                # Fill the detail rows with data (same structure as before)
                detail_rows = []
                for detail in sorted_details:
                    row = [
                        detail.get('invoice no.', ''),
                        detail.get('month', ''),
                        detail.get('invoiced amount', ''),
                        detail.get('payment status', ''),
                        detail.get('paid at', ''),
                        detail.get('total paid', ''),
                        detail.get('outstanding amount', '')
                    ]
                    detail_rows.append(row)

                # Add BALANCE summary row (unchanged)
                balance_row = ['', '', '', '', 'BALANCE:', summary_data.get('outstanding', 0), planet_points]
                detail_rows.append(balance_row)

                # Update the data in the inserted rows
                detail_updates = [{
                    'range': f'{self.SINGLE_TEMPLATE_SHEET}!A17',
                    'values': detail_rows
                }]
                self.sheets_client.batch_update(detail_updates, sheet_id=spreadsheet_id)

                # Apply yellow background to contract header (row 16) and balance row
                balance_row_index = 17 + len(sorted_details) - 1
                self.apply_row_formatting(
                    spreadsheet_id=spreadsheet_id,
                    sheet_name=self.SINGLE_TEMPLATE_SHEET,
                    rows_to_format=[16, balance_row_index],
                    background_color={'red': 1.0, 'green': 0.9, 'blue': 0.6}  # Yellow
                )

            logger.info(f"Single template filled with {len(detail_data) if detail_data else 0} rows")            
            
        except Exception as e:
            logger.error(f"Error filling single template: {e}", exc_info=True)
    
    def fill_multi_template(
        self,
        spreadsheet_id: str,
        contracts_data: List[Dict],
        summary_data: Dict,
        details_by_contract: Dict[str, List[Dict]],
        planet_points: float
    ):
        """
        Fill Multi template with data from multiple contracts using Google Sheets API.
        
        Args:
            spreadsheet_id: Working copy spreadsheet ID
            contracts_data: List of contract information dictionaries
            summary_data: Summary totals dictionary (summed across all contracts)
            details_by_contract: Dictionary mapping contract_id to invoice details
            planet_points: Total planet points
        """
        try:
            # Use first contract's info for customer details
            first_contract = contracts_data[0]
            
            # Prepare batch update requests
            updates = []
            
            # Customer name/company name (A10)
            customer_name = first_contract.get('customer_name') or first_contract.get('company_name', '')
            updates.append({
                'range': f'{self.MULTI_TEMPLATE_SHEET}!A10',
                'values': [[customer_name]]
            })
            
            # Delivery address (A11, A12, A13)
            address = first_contract.get('delivery_address', '')
            line1, line2, line3 = self.parse_delivery_address(address)
            updates.extend([
                {'range': f'{self.MULTI_TEMPLATE_SHEET}!A11', 'values': [[line1]]},
                {'range': f'{self.MULTI_TEMPLATE_SHEET}!A12', 'values': [[line2]]},
                {'range': f'{self.MULTI_TEMPLATE_SHEET}!A13', 'values': [[line3]]}
            ])
            
            # Customer code (I10)
            updates.append({
                'range': f'{self.MULTI_TEMPLATE_SHEET}!I10',
                'values': [[first_contract.get('customer_code', '')]]
            })
            
            # Statement date (I11)
            updates.append({
                'range': f'{self.MULTI_TEMPLATE_SHEET}!I11',
                'values': [[datetime.now(timezone.utc).strftime("%Y-%m-%d")]]
            })
            
            # Summed totals (I12, I13, I14)
            updates.extend([
                {'range': f'{self.MULTI_TEMPLATE_SHEET}!I12', 'values': [[summary_data.get('total_invoiced', 0)]]},
                {'range': f'{self.MULTI_TEMPLATE_SHEET}!I13', 'values': [[summary_data.get('total_paid', 0)]]},
                {'range': f'{self.MULTI_TEMPLATE_SHEET}!I14', 'values': [[summary_data.get('outstanding', 0)]]}
            ])
            
            # Planet points (D29)
            updates.append({
                'range': f'{self.MULTI_TEMPLATE_SHEET}!D29',
                'values': [[planet_points]]
            })

            # Batch update all cells (except invoice details)
            self.sheets_client.batch_update(updates, sheet_id=spreadsheet_id)

            # Dynamic table starting at row 16 (row 16 is header template)
            all_rows = []
            contract_header_rows = []  # Track which rows are contract headers for yellow formatting
            current_row = 17  # Start inserting at row 17

            for contract in contracts_data:
                contract_id = contract.get('contract_id', '')

                # Contract header row
                contract_header = f"{contract_id} | {contract.get('start_date', '')} | {contract.get('end_date', '')}"
                all_rows.append([contract_header, '', '', '', '', '', ''])
                contract_header_rows.append(current_row - 1)  # Track this row index (0-indexed)
                current_row += 1

                # Invoice details for this contract
                details = details_by_contract.get(contract_id, [])
                for detail in details:
                    row = [
                        detail.get('invoice no.', ''),
                        detail.get('month', ''),
                        detail.get('invoiced amount', ''),
                        detail.get('payment status', ''),
                        detail.get('paid at', ''),
                        detail.get('total paid', ''),
                        detail.get('outstanding amount', '')
                    ]
                    all_rows.append(row)
                    current_row += 1

            if all_rows:
                # Add BALANCE summary row
                balance_row = ['', '', '', '', 'BALANCE:', summary_data.get('outstanding', 0), planet_points]
                all_rows.append(balance_row)
                balance_row_index = current_row - 1  # Track balance row (0-indexed)

                # Insert rows for all data (contracts + invoice details + balance)
                num_rows_to_insert = len(all_rows)
                self.insert_rows_with_formatting(
                    spreadsheet_id=spreadsheet_id,
                    sheet_name=self.MULTI_TEMPLATE_SHEET,
                    start_row=17,
                    num_rows=num_rows_to_insert,
                    source_row=15  # Copy formatting from header row 15
                )

                # Fill the inserted rows with data
                detail_updates = [{
                    'range': f'{self.MULTI_TEMPLATE_SHEET}!A17',
                    'values': all_rows
                }]
                self.sheets_client.batch_update(detail_updates, sheet_id=spreadsheet_id)

                # Apply yellow background to contract headers and balance row
                rows_to_highlight = contract_header_rows + [balance_row_index]
                self.apply_row_formatting(
                    spreadsheet_id=spreadsheet_id,
                    sheet_name=self.MULTI_TEMPLATE_SHEET,
                    rows_to_format=rows_to_highlight,
                    background_color={'red': 1.0, 'green': 0.9, 'blue': 0.6}  # Yellow
                )

            logger.info(f"Multi template filled with {len(contracts_data)} contracts")
            
        except Exception as e:
            logger.error(f"Error filling multi template: {e}", exc_info=True)
    
    def delete_sheet_tab(self, spreadsheet_id: str, sheet_name: str):
        """
        Delete a sheet tab from a spreadsheet.
        
        Args:
            spreadsheet_id: Spreadsheet ID
            sheet_name: Name of the sheet to delete
        """
        try:
            # Get sheet info to find the sheet ID
            sheet_info = self.sheets_client.get_sheet_info(spreadsheet_id)
            sheet_id = None
            
            for sheet in sheet_info.get('sheets', []):
                if sheet.get('title') == sheet_name:
                    sheet_id = sheet.get('sheet_id')
                    break
            
            if not sheet_id:
                logger.warning(f"Sheet '{sheet_name}' not found, skipping deletion")
                return
            
            # Use Sheets API batchUpdate to delete the sheet
            service = self.sheets_client.get_service()
            if not service:
                logger.error("Failed to get Sheets service")
                return
            
            request_body = {
                'requests': [{
                    'deleteSheet': {
                        'sheetId': sheet_id
                    }
                }]
            }
            
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=request_body
            ).execute()
            
            logger.info(f"Deleted sheet tab: {sheet_name}")
            
        except Exception as e:
            logger.error(f"Error deleting sheet tab '{sheet_name}': {e}", exc_info=True)

    def export_sheet_as_pdf(self, spreadsheet_id: str, sheet_name: str) -> Optional[bytes]:
        """
        Export a specific sheet as PDF with gridlines hidden.

        Args:
            spreadsheet_id: Spreadsheet ID
            sheet_name: Name of the sheet to export

        Returns:
            PDF bytes or None if failed
        """
        try:
            # Get credentials
            credentials = self.sheets_client.get_credentials()
            if not credentials:
                logger.error("Could not get credentials")
                return None

            # Get sheet GID
            sheet_info = self.sheets_client.get_sheet_info(spreadsheet_id)
            sheet_gid = None

            for sheet in sheet_info.get('sheets', []):
                if sheet.get('title') == sheet_name:
                    sheet_gid = sheet.get('sheet_id')
                    break

            # Build export URL with parameters to hide gridlines
            export_params = {
                'format': 'pdf',
                'size': 'letter',
                'portrait': 'true',
                'fitw': 'true',
                'sheetnames': 'false',
                'printtitle': 'false',
                'pagenumbers': 'false',
                'gridlines': 'false',
                'fzr': 'false',
                'fzc': 'false'
            }

            if sheet_gid is not None:
                export_params['gid'] = sheet_gid

            param_string = '&'.join([f"{k}={v}" for k, v in export_params.items()])
            full_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?{param_string}"

            headers = {
                'Authorization': f'Bearer {credentials.token}'
            }

            import requests
            response = requests.get(full_url, headers=headers, timeout=60)

            if response.status_code == 200:
                logger.info(f"Successfully exported sheet '{sheet_name}' as PDF")
                return response.content
            else:
                logger.error(f"Failed to export PDF: HTTP {response.status_code} - {response.text}")
                return None

        except Exception as e:
            logger.error(f"Error exporting sheet as PDF: {e}", exc_info=True)
            return None

    def cleanup_working_copy(self, spreadsheet_id: str):
        """
        Move the temporary working copy to trash.

        Args:
            spreadsheet_id: Spreadsheet ID to delete
        """
        try:
            credentials = self.sheets_client.get_credentials()
            if not credentials:
                logger.warning("Could not get credentials for cleanup")
                return

            url = f"https://www.googleapis.com/drive/v3/files/{spreadsheet_id}"
            headers = {
                'Authorization': f'Bearer {credentials.token}'
            }
            params = {
                'supportsAllDrives': 'true'
            }

            trash_body = {'trashed': True}

            import requests
            response = requests.patch(url, headers=headers, json=trash_body, params=params, timeout=30)

            if response.status_code == 200:
                logger.info("Successfully moved working copy to trash")
            elif response.status_code == 404:
                logger.info("Working copy already deleted (404)")
            else:
                logger.warning(f"Could not move working copy to trash: HTTP {response.status_code} - {response.text}")

        except Exception as e:
            logger.warning(f"Could not cleanup working copy: {e}")

    def insert_rows_with_formatting(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        start_row: int,
        num_rows: int,
        source_row: int
    ):
        """
        Insert rows and copy formatting from a source row.

        Args:
            spreadsheet_id: Spreadsheet ID
            sheet_name: Name of the sheet
            start_row: Row index where to insert (1-indexed)
            num_rows: Number of rows to insert
            source_row: Row index to copy formatting from (1-indexed)
        """
        try:
            # Get sheet GID
            sheet_info = self.sheets_client.get_sheet_info(spreadsheet_id)
            sheet_gid = None

            for sheet in sheet_info.get('sheets', []):
                if sheet.get('title') == sheet_name:
                    sheet_gid = sheet.get('sheet_id')
                    break

            if sheet_gid is None:
                logger.error(f"Sheet '{sheet_name}' not found")
                return

            # Use Sheets API batchUpdate to insert rows
            service = self.sheets_client.get_service()
            if not service:
                logger.error("Failed to get Sheets service")
                return

            # Build requests
            requests_list = []

            # 1. Insert dimension (rows)
            requests_list.append({
                'insertDimension': {
                    'range': {
                        'sheetId': sheet_gid,
                        'dimension': 'ROWS',
                        'startIndex': start_row - 1,  # Convert to 0-indexed
                        'endIndex': start_row - 1 + num_rows
                    },
                    'inheritFromBefore': False  # Don't inherit from row above
                }
            })

            # 2. Copy formatting from source row to inserted rows
            requests_list.append({
                'copyPaste': {
                    'source': {
                        'sheetId': sheet_gid,
                        'startRowIndex': source_row - 1,  # Convert to 0-indexed
                        'endRowIndex': source_row,
                        'startColumnIndex': 0,
                        'endColumnIndex': 10  # Copy columns A-J
                    },
                    'destination': {
                        'sheetId': sheet_gid,
                        'startRowIndex': start_row - 1,  # Convert to 0-indexed
                        'endRowIndex': start_row - 1 + num_rows,
                        'startColumnIndex': 0,
                        'endColumnIndex': 10
                    },
                    'pasteType': 'PASTE_FORMAT'  # Only copy formatting, not values
                }
            })

            request_body = {
                'requests': requests_list
            }

            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=request_body
            ).execute()

            logger.info(f"Inserted {num_rows} rows at row {start_row} with formatting from row {source_row}")

        except Exception as e:
            logger.error(f"Error inserting rows with formatting: {e}", exc_info=True)

    def apply_row_formatting(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        rows_to_format: List[int],
        background_color: Dict[str, float]
    ):
        """
        Apply background color formatting to specific rows.

        Args:
            spreadsheet_id: Spreadsheet ID
            sheet_name: Name of the sheet
            rows_to_format: List of row indices (0-indexed) to format
            background_color: Dict with 'red', 'green', 'blue' values (0-1)
        """
        try:
            # Get sheet GID
            sheet_info = self.sheets_client.get_sheet_info(spreadsheet_id)
            sheet_gid = None

            for sheet in sheet_info.get('sheets', []):
                if sheet.get('title') == sheet_name:
                    sheet_gid = sheet.get('sheet_id')
                    break

            if sheet_gid is None:
                logger.error(f"Sheet '{sheet_name}' not found")
                return

            # Use Sheets API batchUpdate to apply formatting
            service = self.sheets_client.get_service()
            if not service:
                logger.error("Failed to get Sheets service")
                return

            # Build requests for each row
            requests_list = []

            for row_index in rows_to_format:
                requests_list.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_gid,
                            'startRowIndex': row_index,
                            'endRowIndex': row_index + 1,
                            'startColumnIndex': 0,
                            'endColumnIndex': 7  # Columns A-G
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': background_color
                            }
                        },
                        'fields': 'userEnteredFormat.backgroundColor'
                    }
                })

            request_body = {
                'requests': requests_list
            }

            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=request_body
            ).execute()

            logger.info(f"Applied background color to {len(rows_to_format)} rows")

        except Exception as e:
            logger.error(f"Error applying row formatting: {e}", exc_info=True)
