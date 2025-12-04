"""
Google Sheets helper for chatbot backend

This is basically a wrapper around the Google Sheets API.
Handles:
- Authentication via service account
- Reading/writing cell ranges
- Appending rows, updating cells, bulk updates
- A bit of caching so we don't hammer the API too much
"""

import json, time, requests
from typing import Any, Dict, List, Optional
from datetime import datetime

from .base_client import BaseClient


class GoogleSheetsClient(BaseClient):
    """Wrapper around Google Sheets API (with some caching sprinkled in)."""

    def __init__(self, config_path: Optional[str] = None):
        super().__init__(config_path)

        cfg = self.config.get_sheets_config()
        if not cfg.get("service_account_file"):
            raise ValueError("Google Sheets config missing: service_account_file")

        self.service_account_file = cfg["service_account_file"]
        self.default_sheet_id = cfg.get("default_sheet_id")
        self.cache_ttl = cfg.get("cache_ttl", 60)
        # Drive-related defaults (optional)
        drive_cfg = self.config.get("google_drive", {}) or {}
        # Shared drive / parent folder id where ticket folders should be created
        self.shared_drive_parent_id = drive_cfg.get("shared_drive_id") or drive_cfg.get("shared_drive_parent_id")
        # Retry/backoff settings for Drive uploads
        self.drive_max_retries = int(drive_cfg.get("max_retries", 3))
        self.drive_retry_delay = float(drive_cfg.get("retry_delay", 2.0))

        # token/data caches (token: auth token, data: sheet content)
        self._token_cache: Dict[str, Any] = {}
        self._data_cache: Dict[str, Any] = {}

        # Services cache for API clients
        self._credentials = None
        self._sheets_service = None
        self._drive_service = None

        self.log_info("GoogleSheetsClient ready to roll")

    def _get_access_token(self) -> Optional[str]:
        """Fetch OAuth token from service account. Reuses cached one if not expired."""
        now = time.time()
        token = self._token_cache.get("token")
        exp = self._token_cache.get("expires", 0)

        # if valid token exists, just use it
        if token and now < exp - 60:  # buffer just in case
            return token

        try:
            # NOTE: The file can either be JSON string content or a filepath
            if self.service_account_file.strip().startswith("{"):
                creds_data = json.loads(self.service_account_file)
            else:
                with open(self.service_account_file, "r", encoding="utf-8") as fh:
                    creds_data = json.load(fh)

            from google.oauth2.service_account import Credentials
            from google.auth.transport.requests import Request as GoogleAuthRequest

            creds = Credentials.from_service_account_info(
                creds_data,
                scopes=[
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive",
                ],
            )
            creds.refresh(GoogleAuthRequest())

            # usually valid ~1h, we'll keep a shorter expiry to be safe
            self._token_cache = {
                "token": creds.token,
                "expires": now + 3300,  # 55 min
            }
            return creds.token

        except Exception as e:
            self.log_error("Couldn't get Google Sheets access token", e)
            return None

    def _get_headers(self) -> Optional[Dict[str, str]]:
        token = self._get_access_token()
        if not token:
            return None
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def read_range(
        self, rng: str, sheet_id: Optional[str] = None, use_cache: bool = True
    ) -> List[List[Any]]:
        """Grab some cells from a given range (e.g. 'Sheet1!A1:C10')."""
        sid = sheet_id or self.default_sheet_id
        if not sid:
            self.log_error("read_range: no sheet id configured")
            return []

        cache_key = f"{sid}_{rng}"
        if use_cache and cache_key in self._data_cache:
            cached = self._data_cache[cache_key]
            if time.time() < cached["expires"]:
                return cached["data"]

        try:
            headers = self._get_headers()
            if not headers:
                return []

            url = f"https://sheets.googleapis.com/v4/spreadsheets/{sid}/values/{rng}"
            resp = requests.get(url, headers=headers, timeout=30)

            if resp.status_code == 200:
                body = resp.json()
                values = body.get("values", [])

                if use_cache:
                    self._data_cache[cache_key] = {
                        "data": values,
                        "expires": time.time() + self.cache_ttl,
                    }

                self.log_info(f"Read {len(values)} rows from {rng}")
                return values
            else:
                self.log_error(f"read_range failed ({resp.status_code}): {resp.text}")
                return []
        except Exception as e:
            self.log_error(f"Exception while reading range {rng}", e)
            return []

    def write_range(
        self,
        rng: str,
        rows: List[List[Any]],
        sheet_id: Optional[str] = None,
        value_input_option: str = "RAW",
    ) -> bool:
        """Overwrite a range with data (RAW or USER_ENTERED)."""
        sid = sheet_id or self.default_sheet_id
        if not sid:
            self.log_error("write_range: no sheet id configured")
            return False

        try:
            headers = self._get_headers()
            if not headers:
                return False

            url = f"https://sheets.googleapis.com/v4/spreadsheets/{sid}/values/{rng}"
            payload = {"values": rows, "majorDimension": "ROWS"}
            params = {"valueInputOption": value_input_option}

            resp = requests.put(url, headers=headers, json=payload, params=params)

            if resp.status_code == 200:
                self._invalidate_cache(sid)
                self.log_info(f"Wrote {len(rows)} row(s) to {rng}")
                return True
            else:
                self.log_error(f"write_range failed: {resp.status_code} {resp.text}")
                return False
        except Exception as e:
            self.log_error(f"write_range blew up at {rng}", e)
            return False

    def append_row(
        self,
        sheet_name: str,
        row: List[Any],
        sheet_id: Optional[str] = None,
        value_input_option: str = "RAW",
        setup_dropdowns: Optional[dict] = None,
    ) -> bool:
        """
        Append one row to bottom of sheet.

        Args:
            sheet_name: Name of the sheet
            row: Row data to append
            sheet_id: Optional sheet ID
            value_input_option: "RAW" or "USER_ENTERED"
            setup_dropdowns: Optional dict to set up dropdowns on first append.
                            Format: {col_number: ["option1", "option2", "option3"]}
                            Example: {17: ["Pending", "In Progress", "Done"]}

        Returns:
            bool: Success status
        """
        sid = sheet_id or self.default_sheet_id
        if not sid:
            self.log_error("append_row: no sheet id configured")
            return False

        try:
            headers = self._get_headers()
            if not headers:
                return False

            # Check if this is the first data row (only header exists)
            if setup_dropdowns:
                existing_data = self.read_range(f"{sheet_name}!A:A", sid, use_cache=False)
                if len(existing_data) <= 1:  # Only header row exists
                    self.log_info(f"First data row detected, setting up dropdowns for {sheet_name}")
                    for col, options in setup_dropdowns.items():
                        self.add_data_validation_dropdown(
                            sheet_name=sheet_name,
                            col=col,
                            start_row=2,
                            end_row=1000,
                            values=options,
                            sheet_id=sid
                        )

            url = f"https://sheets.googleapis.com/v4/spreadsheets/{sid}/values/{sheet_name}:append"
            payload = {"values": [row], "majorDimension": "ROWS"}
            params = {"valueInputOption": value_input_option, "insertDataOption": "INSERT_ROWS"}

            resp = requests.post(url, headers=headers, json=payload, params=params)

            if resp.status_code == 200:
                self._invalidate_cache(sid)
                self.log_info(f"Appended row to {sheet_name}")
                return True
            else:
                self.log_error(f"append_row failed: {resp.status_code} {resp.text}")
                return False
        except Exception as e:
            self.log_error(f"append_row exploded for {sheet_name}", e)
            return False

    def find_row(
        self, sheet_name: str, col_idx: int, target: str, sheet_id: Optional[str] = None
    ) -> Optional[int]:
        """Find row number in sheet where given column matches value (1-based index)."""
        data = self.read_range(f"{sheet_name}!A:ZZ", sheet_id)
        for i, row in enumerate(data, start=1):
            if len(row) >= col_idx:
                if str(row[col_idx - 1]).strip() == str(target).strip():
                    return i
        return None  # not found

    def update_cell(
        self, sheet_name: str, row: int, col: int, val: Any, sheet_id: Optional[str] = None
    ) -> bool:
        """Update a single cell (uses write_range under the hood)."""
        col_letter = self._colnum_to_letter(col)
        rng = f"{sheet_name}!{col_letter}{row}"
        return self.write_range(rng, [[val]], sheet_id)

    def batch_update(self, updates: List[Dict[str, Any]], sheet_id: Optional[str] = None, value_input_option: str = "RAW") -> bool:
        """Push multiple updates at once (saves API calls)."""
        sid = sheet_id or self.default_sheet_id
        if not sid or not updates:
            return False

        try:
            headers = self._get_headers()
            if not headers:
                return False

            url = f"https://sheets.googleapis.com/v4/spreadsheets/{sid}/values:batchUpdate"
            payload = {
                "valueInputOption": value_input_option,
                "data": [
                    {"range": upd["range"], "values": upd["values"], "majorDimension": "ROWS"}
                    for upd in updates
                ],
            }

            resp = requests.post(url, headers=headers, json=payload, timeout=30)

            if resp.status_code == 200:
                self._invalidate_cache(sid)
                self.log_info(f"Batch update ok ({len(updates)} updates)")
                return True
            else:
                self.log_error(f"batch_update failed: {resp.status_code} {resp.text}")
                return False
        except Exception as e:
            self.log_error("batch_update error", e)
            return False

    def get_sheet_info(self, sheet_id: Optional[str] = None) -> Dict[str, Any]:
        """Grab metadata about spreadsheet (titles, sheet names, sizes, etc)."""
        sid = sheet_id or self.default_sheet_id
        if not sid:
            return {}

        try:
            headers = self._get_headers()
            if not headers:
                return {}

            url = f"https://sheets.googleapis.com/v4/spreadsheets/{sid}"
            resp = requests.get(url, headers=headers, timeout=30)

            if resp.status_code == 200:
                js = resp.json()
                sheet_info = []
                for s in js.get("sheets", []):
                    props = s.get("properties", {})
                    sheet_info.append(
                        {
                            "sheet_id": props.get("sheetId"),
                            "title": props.get("title"),
                            "sheet_type": props.get("sheetType"),
                            "row_count": props.get("gridProperties", {}).get("rowCount"),
                            "col_count": props.get("gridProperties", {}).get("columnCount"),
                        }
                    )
                return {"spreadsheet_id": sid, "title": js.get("properties", {}).get("title"), "sheets": sheet_info}
            else:
                self.log_error(f"get_sheet_info failed: {resp.status_code}")
                return {}
        except Exception as e:
            self.log_error("get_sheet_info error", e)
            return {}

    def _colnum_to_letter(self, n: int) -> str:
        """Convert col number to spreadsheet letters (1=A, 27=AA, etc)."""
        result = ""
        while n > 0:
            n, rem = divmod(n - 1, 26)
            result = chr(rem + ord("A")) + result
        return result

    def _invalidate_cache(self, sid: str) -> None:
        """Clear cached values for a given sheet id only."""
        bad_keys = [k for k in self._data_cache.keys() if k.startswith(sid)]
        for k in bad_keys:
            del self._data_cache[k]

    def clear_cache(self) -> None:
        """Nuke all caches (token remains)."""
        self._data_cache.clear()
        self.log_info("Cache wiped")

    # === Google Drive Integration ===
    
    def create_drive_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> Optional[str]:
        """Create a folder in Google Drive and return its ID."""
        try:
            headers = self._get_headers()
            if not headers:
                return None
            url = "https://www.googleapis.com/drive/v3/files"

            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }

            params = {}
            # If caller didn't provide a parent, try configured shared drive parent
            effective_parent = parent_folder_id or self.shared_drive_parent_id
            if effective_parent:
                folder_metadata['parents'] = [effective_parent]
                # When creating under a shared drive, include supportsAllDrives
                params['supportsAllDrives'] = 'true'

            resp = requests.post(url, headers=headers, json=folder_metadata, params=params or None, timeout=30)
            
            if resp.status_code == 200:
                folder_data = resp.json()
                folder_id = folder_data.get('id')
                self.log_info(f"Created folder '{folder_name}' with ID: {folder_id}")
                return folder_id
            else:
                self.log_error(f"Failed to create folder: {resp.status_code} {resp.text}")
                return None
                
        except Exception as e:
            self.log_error(f"Exception creating folder {folder_name}", e)
            return None
    
    def upload_file_to_drive(self, file_data: bytes, filename: str, 
                           folder_id: Optional[str] = None, 
                           mime_type: str = "application/octet-stream") -> Optional[str]:
        """Upload a file to Google Drive and return its ID."""
        # We'll implement retries with exponential backoff
        import time as _time
        try:
            headers = self._get_headers()
            if not headers:
                return None

            # Step 1: Create file metadata
            metadata = {'name': filename}
            effective_folder = folder_id or self.shared_drive_parent_id
            if effective_folder:
                metadata['parents'] = [effective_folder]

            # Simpler approach - use requests multipart
            files = {
                'metadata': (None, json.dumps(metadata), 'application/json'),
                'file': (filename, file_data, mime_type)
            }

            # Remove Content-Type from headers to let requests set it for multipart
            upload_headers = {k: v for k, v in headers.items() if k.lower() != 'content-type'}

            params = {'uploadType': 'multipart'}
            # Include supportsAllDrives when uploading into a folder (shared drive)
            if effective_folder:
                params['supportsAllDrives'] = 'true'

            url = "https://www.googleapis.com/upload/drive/v3/files"

            attempt = 0
            while attempt <= self.drive_max_retries:
                attempt += 1
                try:
                    resp = requests.post(url, headers=upload_headers, files=files, params=params, timeout=60)
                    if resp.status_code == 200:
                        file_data_resp = resp.json()
                        file_id = file_data_resp.get('id')
                        self.log_info(f"Uploaded file '{filename}' with ID: {file_id} (attempt {attempt})")
                        return file_id
                    else:
                        # For 4xx errors, don't retry except for 429
                        if 400 <= resp.status_code < 500 and resp.status_code != 429:
                            self.log_error(f"Failed to upload file (status {resp.status_code}): {resp.text}")
                            return None
                        self.log_warning(f"Transient upload error (status {resp.status_code}) - attempt {attempt}/{self.drive_max_retries}")
                except Exception as exc:
                    self.log_warning(f"Exception during upload attempt {attempt}: {exc}")

                # If not returned, sleep then retry
                if attempt <= self.drive_max_retries:
                    backoff = self.drive_retry_delay * (2 ** (attempt - 1))
                    _time.sleep(backoff)

            self.log_error(f"Exceeded max retries ({self.drive_max_retries}) uploading file {filename}")
            return None
        except Exception as e:
            self.log_error(f"Exception uploading file {filename}", e)
            return None
    
    def get_drive_file_link(self, file_id: str, make_public: bool = True) -> Optional[str]:
        """Get a shareable link for a Google Drive file."""
        try:
            if make_public:
                # First, make the file publicly viewable
                self._make_file_public(file_id)
            
            # Return the view link
            return f"https://drive.google.com/file/d/{file_id}/view"
            
        except Exception as e:
            self.log_error(f"Exception getting file link for {file_id}", e)
            return None
    
    def _make_file_public(self, file_id: str) -> bool:
        """Make a Google Drive file publicly viewable."""
        try:
            headers = self._get_headers()
            if not headers:
                return False
            
            url = f"https://www.googleapis.com/drive/v3/files/{file_id}/permissions"
            
            permission = {
                'role': 'reader',
                'type': 'anyone'
            }
            
            resp = requests.post(url, headers=headers, json=permission, timeout=30)
            
            if resp.status_code == 200:
                self.log_info(f"Made file {file_id} publicly viewable")
                return True
            else:
                self.log_warning(f"Could not make file public: {resp.status_code}")
                return False
                
        except Exception as e:
            self.log_error(f"Exception making file {file_id} public", e)
            return False

    # === QnA SEARCH FUNCTIONALITY ===
    
    def search_qna(self, sheet_id: str, query: str, fallback_data: Optional[List[Dict]] = None) -> str:
        """
        Search QnA knowledge base with intelligent matching and fallback.
        
        Args:
            sheet_id: Google Sheet ID containing QnA data
            query: User's search query
            fallback_data: Local QnA data to use if Google Sheets fails
            
        Returns:
            Formatted answer string
        """
        try:
            self.log_info(f"Searching QnA for query: {query[:50]}...")
            
            # Try Google Sheets first
            try:
                sheet_data = self.get_range(sheet_id, "A:E")  # Assuming columns A-E contain QnA data
                if sheet_data and len(sheet_data) > 1:  # Has header + data
                    result = self._search_sheet_qna(sheet_data, query)
                    if result:
                        return self._format_qna_result(result)
            except Exception as e:
                self.log_warning(f"Google Sheets QnA search failed: {e}")
            
            # Fallback to local data
            if fallback_data:
                self.log_info("Using fallback QnA data")
                result = self._search_local_qna(fallback_data, query)
                if result:
                    return self._format_qna_result(result)
            
            return "I couldn't find a specific answer to your question. Please contact our support team for assistance."
            
        except Exception as e:
            self.log_error(f"Error in QnA search for query '{query}'", e)
            return "I encountered an error searching for information. Please try again or contact support."
    
    def _search_sheet_qna(self, sheet_data: List[List], query: str) -> Optional[Dict]:
        """Search through Google Sheets QnA data."""
        if not sheet_data or len(sheet_data) < 2:
            return None
        
        query_lower = query.lower()
        query_words = [word.strip('.,?!') for word in query_lower.split() if len(word.strip('.,?!')) > 2]
        
        best_match = None
        best_score = 0
        
        # Skip header row, process data rows
        for row in sheet_data[1:]:
            if len(row) < 4:  # Need at least ID, Category, Question, Answer
                continue
            
            # Extract fields (adjust indices based on your sheet structure)
            qna_id = str(row[0]) if len(row) > 0 else ""
            category = str(row[1]).lower() if len(row) > 1 else ""
            question = str(row[2]).lower() if len(row) > 2 else ""
            answer = str(row[3]) if len(row) > 3 else ""
            keywords = str(row[4]).lower() if len(row) > 4 else ""
            
            score = self._calculate_qna_score(query_lower, query_words, question, keywords, category)
            
            if score > best_score:
                best_score = score
                best_match = {
                    'qna_id': qna_id,
                    'category': row[1] if len(row) > 1 else "",
                    'question': row[2] if len(row) > 2 else "",
                    'answer': answer,
                    'keywords': row[4] if len(row) > 4 else "",
                    'score': score
                }
        
        return best_match if best_score >= 3 else None  # Minimum score threshold
    
    def _search_local_qna(self, qna_data: List[Dict], query: str) -> Optional[Dict]:
        """Search through local fallback QnA data."""
        query_lower = query.lower()
        query_words = [word.strip('.,?!') for word in query_lower.split() if len(word.strip('.,?!')) > 2]
        
        best_match = None
        best_score = 0
        
        for item in qna_data:
            question = str(item.get('Question', '')).lower()
            keywords = str(item.get('Keywords', item.get('Sub-category/Keywords', ''))).lower()
            category = str(item.get('Category', '')).lower()
            
            score = self._calculate_qna_score(query_lower, query_words, question, keywords, category)
            
            if score > best_score:
                best_score = score
                best_match = {
                    'qna_id': item.get('QnA ID', ''),
                    'category': item.get('Category', ''),
                    'question': item.get('Question', ''),
                    'answer': item.get('Answer', ''),
                    'keywords': keywords,
                    'score': score
                }
        
        return best_match if best_score >= 3 else None
    
    def _calculate_qna_score(self, query_lower: str, query_words: List[str], 
                           question: str, keywords: str, category: str) -> int:
        """Calculate relevance score for QnA matching."""
        score = 0
        
        # Exact phrase matching (highest priority)
        if query_lower in question:
            score += 10
        if query_lower in keywords:
            score += 8
        if query_lower in category:
            score += 5
        
        # Word-by-word matching
        question_words = question.split()
        keyword_words = keywords.replace(',', ' ').split()
        category_words = category.split()
        
        for query_word in query_words:
            # Direct word matching
            if query_word in question_words:
                score += 3
            if query_word in keyword_words:
                score += 3
            if query_word in category_words:
                score += 2
            
            # Enhanced word variations for maintenance domain
            variations = self._get_word_variations(query_word)
            for variation in variations:
                if variation in question:
                    score += 4
                if variation in keywords:
                    score += 4
                if variation in category:
                    score += 2
        
        return score
    
    def _get_word_variations(self, word: str) -> List[str]:
        """Get word variations for better matching."""
        variations = {
            # Air Conditioning
            'aircond': ['aircon', 'aircond', 'air-cond', 'air conditioning', 'ac', 'a/c', 'cooling'],
            'aircon': ['aircond', 'air-cond', 'air conditioning', 'ac', 'a/c', 'cooling'],
            'ac': ['aircon', 'aircond', 'air conditioning', 'cooling'],
            
            # Water & Plumbing
            'leak': ['leaking', 'leaked', 'leaks', 'drip', 'dripping', 'water problem'],
            'water': ['h2o', 'water supply', 'water pressure', 'tap', 'faucet'],
            'plumbing': ['pipe', 'pipes', 'drain', 'drainage', 'sewage', 'toilet'],
            
            # Electrical
            'electricity': ['electric', 'electrical', 'power', 'trip', 'tripped', 'blackout'],
            'electrical': ['electric', 'electricity', 'power', 'wiring', 'socket', 'plug'],
            'trip': ['tripped', 'circuit breaker', 'main switch', 'db box', 'fuse'],
            
            # Internet & Connectivity
            'wifi': ['wi-fi', 'wireless', 'internet', 'connection', 'network', 'online'],
            'internet': ['wifi', 'wi-fi', 'connection', 'network', 'online', 'connectivity'],
            'connection': ['connect', 'connectivity', 'network', 'signal'],
            
            # Access & Security
            'key': ['keys', 'door key', 'room key', 'spare key', 'access'],
            'card': ['access card', 'keycard', 'lift card', 'scan card', 'building card'],
            'lock': ['locked', 'unlock', 'door lock', 'digital lock', 'smart lock'],
            
            # Maintenance & Repair
            'repair': ['fix', 'fixing', 'broken', 'damaged', 'not working', 'malfunction'],
            'maintenance': ['maintain', 'service', 'upkeep', 'servicing', 'repair'],
            'broken': ['damaged', 'not working', 'spoilt', 'faulty', 'malfunction'],
            
            # Common Issues
            'noise': ['noisy', 'loud', 'sound', 'disturbance'],
            'smell': ['odor', 'stink', 'bad smell', 'smelly'],
            'hot': ['warm', 'heating', 'temperature', 'too hot'],
            'cold': ['cool', 'cooling', 'chilly', 'not cold']
        }
        
        return variations.get(word, [word])
    
    def _format_qna_result(self, result: Dict) -> str:
        """Format QnA search result for display (insides style: show full answer, no forced bullets or truncation)."""
        if not result:
            return "No answer found."
        question = result.get('question', '')
        answer = result.get('answer', '')
        # Show question and full answer, no forced bullet points or truncation
        return f"{answer.strip()}"
    
    def _make_answer_concise(self, answer: str, max_points: int = 4, max_words_per_point: int = 15) -> str:
        """(Insides style) Return answer as-is, no forced bullet points or truncation."""
        return answer.strip() if answer else ""

    # === RICH TEXT AND FORMATTING ===
    
    def append_rich_link_labelled(self, sheet_name: str, row: List[Any], 
                                link_col_idx: int, link_url: str, link_text: str, 
                                sheet_id: Optional[str] = None) -> bool:
        """
        Append a row with a rich text link in specified column.
        
        Args:
            sheet_name: Name of the sheet to append to
            row: Row data as list
            link_col_idx: Column index (0-based) where link should be placed
            link_url: URL for the hyperlink
            link_text: Display text for the link
            sheet_id: Optional sheet ID
            
        Returns:
            bool: Success status
        """
        try:
            # First append the basic row
            if not self.append_row(sheet_name, row, sheet_id):
                return False
            
            # Then update the link cell with rich text
            # Get the last row number by reading the sheet
            data = self.read_range(f"{sheet_name}!A:A", sheet_id, use_cache=False)
            last_row = len(data)
            
            return self.update_rich_link_cell(sheet_name, last_row, link_col_idx + 1, 
                                            link_url, link_text, sheet_id)
                                            
        except Exception as e:
            self.log_error(f"Failed to append row with rich link in {sheet_name}", e)
            return False
    
    def update_rich_link_cell(self, sheet_name: str, row: int, col: int, 
                            link_url: str, link_text: str, 
                            sheet_id: Optional[str] = None) -> bool:
        """
        Update a cell with a rich text hyperlink.
        
        Args:
            sheet_name: Name of the sheet
            row: Row number (1-based)
            col: Column number (1-based)
            link_url: URL for the hyperlink
            link_text: Display text for the link
            sheet_id: Optional sheet ID
            
        Returns:
            bool: Success status
        """
        sid = sheet_id or self.default_sheet_id
        if not sid:
            self.log_error("update_rich_link_cell: no sheet id configured")
            return False
            
        try:
            headers = self._get_headers()
            if not headers:
                return False
            
            col_letter = self._colnum_to_letter(col)
            cell_range = f"{sheet_name}!{col_letter}{row}"
            
            # Use batchUpdate with rich text format
            url = f"https://sheets.googleapis.com/v4/spreadsheets/{sid}:batchUpdate"
            
            # Get sheet ID for the specific sheet name
            sheet_info = self.get_sheet_info(sid)
            target_sheet_id = None
            for sheet in sheet_info.get('sheets', []):
                if sheet.get('title') == sheet_name:
                    target_sheet_id = sheet.get('sheet_id')
                    break
            
            if target_sheet_id is None:
                self.log_error(f"Could not find sheet ID for sheet: {sheet_name}")
                return False
            
            requests_payload = [{
                "updateCells": {
                    "rows": [{
                        "values": [{
                            "userEnteredValue": {
                                "formulaValue": f'=HYPERLINK("{link_url}","{link_text}")'
                            }
                        }]
                    }],
                    "fields": "userEnteredValue",
                    "start": {
                        "sheetId": target_sheet_id,
                        "rowIndex": row - 1,
                        "columnIndex": col - 1
                    }
                }
            }]
            
            payload = {"requests": requests_payload}
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if resp.status_code == 200:
                self._invalidate_cache(sid)
                self.log_info(f"Updated cell {cell_range} with rich link: {link_text}")
                return True
            else:
                self.log_error(f"update_rich_link_cell failed: {resp.status_code} {resp.text}")
                return False
                
        except Exception as e:
            self.log_error(f"Exception updating rich link cell {sheet_name}!{col_letter}{row}", e)
            return False

    def update_file_link_and_timestamp(self, sheet_name: str, row: int, 
                                     link_col: int, timestamp_col: int,
                                     link_url: str, link_text: str,
                                     sheet_id: Optional[str] = None,
                                     timestamp_format: str = "%Y-%m-%d %H:%M:%S") -> bool:
        """
        Update a cell with a file link and automatically update timestamp in another column.
        
        Args:
            sheet_name: Name of the sheet
            row: Row number (1-based)
            link_col: Column number for the link (1-based)
            timestamp_col: Column number for the timestamp (1-based)
            link_url: URL for the file link
            link_text: Display text for the link
            sheet_id: Optional sheet ID
            timestamp_format: Format string for timestamp
            
        Returns:
            bool: Success status
        """
        try:
            # Update the link cell
            if not self.update_rich_link_cell(sheet_name, row, link_col, link_url, link_text, sheet_id):
                return False
            
            # Update timestamp in the specified column
            current_time = datetime.now().strftime(timestamp_format)
            return self.update_cell(sheet_name, row, timestamp_col, current_time, sheet_id)
            
        except Exception as e:
            self.log_error(f"Failed to update file link and timestamp in {sheet_name} row {row}", e)
            return False

    def update_cell_with_timestamp(self, sheet_name: str, row: int, value_col: int, 
                                 timestamp_col: int, value: Any,
                                 sheet_id: Optional[str] = None,
                                 timestamp_format: str = "%Y-%m-%d %H:%M:%S") -> bool:
        """
        Update a cell value and automatically update timestamp in another column.
        
        Args:
            sheet_name: Name of the sheet
            row: Row number (1-based)
            value_col: Column number for the value (1-based)
            timestamp_col: Column number for the timestamp (1-based)
            value: Value to set in the cell
            sheet_id: Optional sheet ID
            timestamp_format: Format string for timestamp
            
        Returns:
            bool: Success status
        """
        try:
            # Batch update both cells for efficiency
            updates = []
            
            # Add value update
            value_col_letter = self._colnum_to_letter(value_col)
            value_range = f"{sheet_name}!{value_col_letter}{row}"
            updates.append({
                "range": value_range,
                "values": [[value]]
            })
            
            # Add timestamp update
            timestamp_col_letter = self._colnum_to_letter(timestamp_col)
            timestamp_range = f"{sheet_name}!{timestamp_col_letter}{row}"
            current_time = datetime.now().strftime(timestamp_format)
            updates.append({
                "range": timestamp_range,
                "values": [[current_time]]
            })
            
            return self.batch_update(updates, sheet_id)
            
        except Exception as e:
            self.log_error(f"Failed to update cell with timestamp in {sheet_name} row {row}", e)
            return False

    # === DYNAMIC SHEET OPERATIONS ===
    
    def resolve_sheet_name(self, base_name: str, sheet_id: Optional[str] = None,
                         date_format: str = "%Y%m", fallback_suffix: str = "_backup") -> str:
        """
        Resolve sheet name with date prefix/suffix and fallback options.
        Useful for sheets that follow naming patterns like "Data_202409", "Log_2024_Sep", etc.
        
        Args:
            base_name: Base name of the sheet (e.g., "Data", "Log")
            sheet_id: Optional sheet ID
            date_format: Format for date suffix (e.g., "%Y%m" for "202409")
            fallback_suffix: Suffix to add if date-based sheet doesn't exist
            
        Returns:
            str: Resolved sheet name that exists in the spreadsheet
        """
        try:
            sheet_info = self.get_sheet_info(sheet_id)
            available_sheets = [sheet.get('title', '') for sheet in sheet_info.get('sheets', [])]
            
            if not available_sheets:
                self.log_warning("No sheets found in spreadsheet")
                return base_name
            
            # Try different date-based naming patterns
            current_date = datetime.now()
            
            patterns_to_try = [
                f"{base_name}_{current_date.strftime(date_format)}",  # Data_202409
                f"{current_date.strftime(date_format)}_{base_name}",  # 202409_Data
                f"{base_name}-{current_date.strftime(date_format)}",  # Data-202409
                f"{base_name}_{current_date.strftime('%Y_%m')}",      # Data_2024_09
                f"{base_name}_{current_date.strftime('%b_%Y')}",      # Data_Sep_2024
                f"{base_name}_{current_date.strftime('%Y')}",         # Data_2024
                base_name,  # Exact match
                f"{base_name}{fallback_suffix}",  # With fallback suffix
            ]
            
            # Check each pattern
            for pattern in patterns_to_try:
                if pattern in available_sheets:
                    self.log_info(f"Resolved sheet name: {pattern}")
                    return pattern
            
            # Try partial matching (case insensitive)
            base_lower = base_name.lower()
            for sheet_name in available_sheets:
                if base_lower in sheet_name.lower() or sheet_name.lower().startswith(base_lower):
                    self.log_info(f"Resolved sheet name via partial match: {sheet_name}")
                    return sheet_name
            
            # Last resort - return first available sheet
            first_sheet = available_sheets[0]
            self.log_warning(f"Could not resolve sheet name for '{base_name}', using first available: {first_sheet}")
            return first_sheet
            
        except Exception as e:
            self.log_error(f"Error resolving sheet name for '{base_name}'", e)
            return base_name

    def get_sheet_names(self, sheet_id: Optional[str] = None) -> List[str]:
        """
        Get list of all sheet names in the spreadsheet.
        
        Args:
            sheet_id: Optional sheet ID
            
        Returns:
            List[str]: List of sheet names
        """
        try:
            sheet_info = self.get_sheet_info(sheet_id)
            return [sheet.get('title', '') for sheet in sheet_info.get('sheets', [])]
        except Exception as e:
            self.log_error("Error getting sheet names", e)
            return []

    def sheet_exists(self, sheet_name: str, sheet_id: Optional[str] = None) -> bool:
        """
        Check if a sheet with the given name exists.
        
        Args:
            sheet_name: Name of the sheet to check
            sheet_id: Optional sheet ID
            
        Returns:
            bool: True if sheet exists, False otherwise
        """
        try:
            sheet_names = self.get_sheet_names(sheet_id)
            return sheet_name in sheet_names
        except Exception as e:
            self.log_error(f"Error checking if sheet '{sheet_name}' exists", e)
            return False

    # === BULK DATA OPERATIONS ===
    
    def append_bulk_data(self, sheet_name: str, data_rows: List[List[Any]], 
                        sheet_id: Optional[str] = None,
                        value_input_option: str = "RAW",
                        validate_data: bool = True,
                        chunk_size: int = 1000) -> bool:
        """
        Append multiple rows efficiently with optional validation.
        
        Args:
            sheet_name: Name of the sheet to append to
            data_rows: List of row data (each row is a list of values)
            sheet_id: Optional sheet ID
            value_input_option: "RAW" or "USER_ENTERED"
            validate_data: Whether to validate data before insertion
            chunk_size: Number of rows to process in each batch
            
        Returns:
            bool: Success status
        """
        if not data_rows:
            self.log_warning("append_bulk_data: no data provided")
            return True
        
        try:
            # Validate data if requested
            if validate_data:
                for i, row in enumerate(data_rows):
                    if not self.validate_row_data(row):
                        self.log_error(f"Data validation failed for row {i}: {row}")
                        return False
            
            # Process data in chunks for large datasets
            total_rows = len(data_rows)
            success = True
            
            for i in range(0, total_rows, chunk_size):
                chunk = data_rows[i:i + chunk_size]
                chunk_success = self._append_data_chunk(sheet_name, chunk, sheet_id, value_input_option)
                if not chunk_success:
                    success = False
                    break
                
                self.log_info(f"Processed chunk {i//chunk_size + 1}: {len(chunk)} rows")
            
            if success:
                self.log_info(f"Successfully appended {total_rows} rows to {sheet_name}")
            
            return success
            
        except Exception as e:
            self.log_error(f"Error in bulk data append to {sheet_name}", e)
            return False
    
    def _append_data_chunk(self, sheet_name: str, chunk: List[List[Any]], 
                          sheet_id: Optional[str], value_input_option: str) -> bool:
        """Helper method to append a chunk of data."""
        sid = sheet_id or self.default_sheet_id
        if not sid:
            return False
        
        try:
            headers = self._get_headers()
            if not headers:
                return False

            url = f"https://sheets.googleapis.com/v4/spreadsheets/{sid}/values/{sheet_name}:append"
            payload = {"values": chunk, "majorDimension": "ROWS"}
            params = {"valueInputOption": value_input_option, "insertDataOption": "INSERT_ROWS"}

            resp = requests.post(url, headers=headers, json=payload, params=params, timeout=60)

            if resp.status_code == 200:
                self._invalidate_cache(sid)
                return True
            else:
                self.log_error(f"_append_data_chunk failed: {resp.status_code} {resp.text}")
                return False
                
        except Exception as e:
            self.log_error(f"Exception in _append_data_chunk", e)
            return False

    def bulk_update_cells(self, updates: List[Dict[str, Any]], 
                         sheet_id: Optional[str] = None,
                         chunk_size: int = 100) -> bool:
        """
        Perform bulk cell updates efficiently.
        
        Args:
            updates: List of update dictionaries with 'sheet_name', 'row', 'col', 'value'
            sheet_id: Optional sheet ID
            chunk_size: Number of updates to process in each batch
            
        Returns:
            bool: Success status
        """
        if not updates:
            return True
        
        try:
            # Group updates by batch for efficiency
            total_updates = len(updates)
            success = True
            
            for i in range(0, total_updates, chunk_size):
                chunk = updates[i:i + chunk_size]
                
                # Convert to batch update format
                batch_updates = []
                for update in chunk:
                    sheet_name = update.get('sheet_name', '')
                    row = update.get('row', 1)
                    col = update.get('col', 1)
                    value = update.get('value', '')
                    
                    col_letter = self._colnum_to_letter(col)
                    range_name = f"{sheet_name}!{col_letter}{row}"
                    
                    batch_updates.append({
                        "range": range_name,
                        "values": [[value]]
                    })
                
                chunk_success = self.batch_update(batch_updates, sheet_id)
                if not chunk_success:
                    success = False
                    break
                
                self.log_info(f"Processed update chunk {i//chunk_size + 1}: {len(chunk)} updates")
            
            if success:
                self.log_info(f"Successfully processed {total_updates} bulk updates")
            
            return success
            
        except Exception as e:
            self.log_error("Error in bulk cell updates", e)
            return False

    # === DATA VALIDATION UTILITIES ===
    
    def validate_row_data(self, row: List[Any], max_col_count: int = 50, 
                         max_cell_length: int = 50000) -> bool:
        """
        Validate row data before insertion into Google Sheets.
        
        Args:
            row: Row data to validate
            max_col_count: Maximum number of columns allowed
            max_cell_length: Maximum length per cell value
            
        Returns:
            bool: True if data is valid, False otherwise
        """
        try:
            if not isinstance(row, list):
                self.log_error(f"Row data must be a list, got {type(row)}")
                return False
            
            if len(row) > max_col_count:
                self.log_error(f"Row has too many columns: {len(row)} > {max_col_count}")
                return False
            
            for i, cell in enumerate(row):
                # Convert to string to check length
                cell_str = str(cell) if cell is not None else ""
                
                if len(cell_str) > max_cell_length:
                    self.log_error(f"Cell {i} exceeds maximum length: {len(cell_str)} > {max_cell_length}")
                    return False
                
                # Check for problematic characters that might break Google Sheets
                if '\x00' in cell_str:  # Null character
                    self.log_error(f"Cell {i} contains null character")
                    return False
            
            return True
            
        except Exception as e:
            self.log_error("Error validating row data", e)
            return False
    
    def sanitize_row_data(self, row: List[Any], max_cell_length: int = 50000) -> List[Any]:
        """
        Sanitize row data for safe insertion into Google Sheets.
        
        Args:
            row: Row data to sanitize
            max_cell_length: Maximum length per cell value
            
        Returns:
            List[Any]: Sanitized row data
        """
        try:
            if not isinstance(row, list):
                return []
            
            sanitized = []
            for cell in row:
                if cell is None:
                    sanitized.append("")
                else:
                    # Convert to string and sanitize
                    cell_str = str(cell)
                    
                    # Remove null characters
                    cell_str = cell_str.replace('\x00', '')
                    
                    # Truncate if too long
                    if len(cell_str) > max_cell_length:
                        cell_str = cell_str[:max_cell_length - 3] + "..."
                    
                    # Handle special cases for numbers/booleans
                    if isinstance(cell, (int, float, bool)):
                        sanitized.append(cell)
                    else:
                        sanitized.append(cell_str)
            
            return sanitized
            
        except Exception as e:
            self.log_error("Error sanitizing row data", e)
            return []
    
    def validate_sheet_range(self, range_str: str) -> bool:
        """
        Validate a Google Sheets range string.
        
        Args:
            range_str: Range string like "Sheet1!A1:B10" or "A1:B10"
            
        Returns:
            bool: True if range format is valid
        """
        try:
            import re
            
            # Pattern for sheet range: [SheetName!]A1:B10 or [SheetName!]A:B or [SheetName!]1:10
            pattern = r'^(?:[^!]+!)?[A-Z]+\d*:[A-Z]+\d*$|^(?:[^!]+!)?[A-Z]+:[A-Z]+$|^(?:[^!]+!)?\d+:\d+$'
            
            if re.match(pattern, range_str.strip()):
                return True
            else:
                self.log_error(f"Invalid range format: {range_str}")
                return False
                
        except Exception as e:
            self.log_error(f"Error validating range '{range_str}'", e)
            return False

    def get_range(self, sheet_id: str, range_str: str, use_cache: bool = True) -> List[List[Any]]:
        """
        Alternative method name for read_range (for compatibility with fg-chatbot-02 style).
        
        Args:
            sheet_id: Sheet ID
            range_str: Range string
            use_cache: Whether to use cache
            
        Returns:
            List[List[Any]]: Sheet data
        """
        return self.read_range(range_str, sheet_id, use_cache)

    # === INVOICE TEMPLATE & CONFIG METHODS ===

    def get_config_value(self, key: str, sheet_name: str = "Config", sheet_id: Optional[str] = None) -> Optional[str]:
        """
        Get a configuration value from the Config sheet.
        
        Args:
            key: Configuration key to look for (case-insensitive)
            sheet_name: Name of the config sheet (default: "Config")
            sheet_id: Optional sheet ID (uses default if not provided)
            
        Returns:
            Configuration value or None if not found
        """
        try:
            sid = sheet_id or self.default_sheet_id
            if not sid:
                self.log_error("get_config_value: no sheet id configured")
                return None
            
            # Read Config sheet (A:B columns, first 50 rows should be enough)
            rows = self.read_range(f"{sheet_name}!A1:B50", sid, use_cache=False)
            if not rows:
                self.log_warning(f"Config sheet '{sheet_name}' is empty")
                return None
            
            # Search for the key (case-insensitive)
            key_lower = key.strip().lower()
            for row in rows:
                if len(row) >= 2:
                    row_key = str(row[0]).strip().lower()
                    if row_key == key_lower:
                        value = str(row[1]).strip()
                        self.log_info(f"Found config value for '{key}': {value}")
                        return value
            
            self.log_warning(f"Config key '{key}' not found in sheet '{sheet_name}'")
            return None
            
        except Exception as e:
            self.log_error(f"Error getting config value '{key}'", e)
            return None

    def update_config_value(self, key: str, value: str, sheet_name: str = "Config", sheet_id: Optional[str] = None) -> bool:
        """
        Update a configuration value in the Config sheet.
        If the key doesn't exist, it will be appended as a new row.
        
        Args:
            key: Configuration key to update
            value: New value to set
            sheet_name: Name of the config sheet (default: "Config")
            sheet_id: Optional sheet ID (uses default if not provided)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            sid = sheet_id or self.default_sheet_id
            if not sid:
                self.log_error("update_config_value: no sheet id configured")
                return False
            
            # Read current config
            rows = self.read_range(f"{sheet_name}!A1:B50", sid, use_cache=False)
            if not rows:
                # Empty sheet, append first row
                success = self.append_row(sheet_name, [key, value], sid)
                if success:
                    self.log_info(f"Added new config key '{key}' with value '{value}'")
                return success
            
            # Search for existing key
            key_lower = key.strip().lower()
            for i, row in enumerate(rows):
                if len(row) >= 1:
                    row_key = str(row[0]).strip().lower()
                    if row_key == key_lower:
                        # Found the key, update its value
                        row_index = i + 1  # 1-based index
                        range_str = f"{sheet_name}!B{row_index}:B{row_index}"
                        success = self.write_range(range_str, [[value]], sid, value_input_option="USER_ENTERED")
                        if success:
                            self.log_info(f"Updated config key '{key}' to value '{value}'")
                            # Invalidate cache for this sheet
                            self._invalidate_cache(sid)
                        return success
            
            # Key not found, append new row
            success = self.append_row(sheet_name, [key, value], sid)
            if success:
                self.log_info(f"Added new config key '{key}' with value '{value}'")
            return success
            
        except Exception as e:
            self.log_error(f"Error updating config value '{key}'", e)
            return False

    def read_template_sheet(self, sheet_name: str = "Invoice Template", sheet_id: Optional[str] = None) -> Optional[List[List[Any]]]:
        """
        Read all data from a template sheet (e.g., Invoice Template).

        Args:
            sheet_name: Name of the template sheet (default: "Invoice Template")
            sheet_id: Optional sheet ID (uses default if not provided)

        Returns:
            List of rows from the sheet, or None if failed
        """
        try:
            sid = sheet_id or self.default_sheet_id
            if not sid:
                self.log_error("read_template_sheet: no sheet id configured")
                return None

            # Read a large range to capture the entire template (A1:Z100)
            rows = self.read_range(f"{sheet_name}!A1:Z100", sid, use_cache=False)
            if rows:
                self.log_info(f"Read {len(rows)} rows from template sheet '{sheet_name}'")
                return rows
            else:
                self.log_warning(f"No data found in template sheet '{sheet_name}'")
                return None

        except Exception as e:
            self.log_error(f"Error reading template sheet '{sheet_name}'", e)
            return None

    # === GOOGLE API SERVICES (for template-based PDF generation) ===

    def get_credentials(self):
        """
        Get Google API credentials for service account.

        This method ensures credentials are always fresh by:
        1. Creating new credentials if not cached
        2. Refreshing expired credentials before returning
        3. Proactively refreshing service account tokens

        Returns:
            Credentials object with valid token
        """
        try:
            # Create credentials if not cached
            if self._credentials is None:
                from google.oauth2.service_account import Credentials

                # Load service account credentials
                if self.service_account_file.strip().startswith("{"):
                    creds_data = json.loads(self.service_account_file)
                else:
                    with open(self.service_account_file, "r", encoding="utf-8") as fh:
                        creds_data = json.load(fh)

                self._credentials = Credentials.from_service_account_info(
                    creds_data,
                    scopes=[
                        "https://www.googleapis.com/auth/spreadsheets",
                        "https://www.googleapis.com/auth/drive",
                    ],
                )
                self.log_info("Credentials initialized")

            # IMPORTANT: Always refresh credentials before use to ensure valid token
            # Service account credentials need to be refreshed to get an access token
            import google.auth.transport.requests as auth_requests

            # Check if credentials need refresh (expired or no token yet)
            if not self._credentials.valid:
                self.log_info("Refreshing credentials (expired or not yet valid)")
                self._credentials.refresh(auth_requests.Request())
                self.log_info("Credentials refreshed successfully")

            return self._credentials

        except Exception as e:
            self.log_error("Error getting/refreshing credentials", e)
            # Clear cached credentials on error so next call will retry
            self._credentials = None
            return None

    def get_service(self):
        """
        Get Google Sheets API service client.

        Returns:
            Sheets service object
        """
        if self._sheets_service is None:
            try:
                from googleapiclient.discovery import build

                credentials = self.get_credentials()
                if not credentials:
                    return None

                self._sheets_service = build('sheets', 'v4', credentials=credentials)
                self.log_info("Sheets service initialized")

            except Exception as e:
                self.log_error("Error creating Sheets service", e)
                return None

        return self._sheets_service

    def get_drive_service(self):
        """
        Get Google Drive API service client.

        Returns:
            Drive service object
        """
        if self._drive_service is None:
            try:
                from googleapiclient.discovery import build

                credentials = self.get_credentials()
                if not credentials:
                    return None

                self._drive_service = build('drive', 'v3', credentials=credentials)
                self.log_info("Drive service initialized")

            except Exception as e:
                self.log_error("Error creating Drive service", e)
                return None

        return self._drive_service