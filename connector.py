import html
import time
import requests
import json
from datetime import datetime
from datetime import timezone
from typing import Any, List

from bs4 import BeautifulSoup
from danswer.configs.app_configs import INDEX_BATCH_SIZE
from danswer.configs.constants import DocumentSource
from danswer.connectors.interfaces import GenerateDocumentsOutput
from danswer.connectors.interfaces import PollConnector
from danswer.connectors.interfaces import SecondsSinceUnixEpoch
from danswer.connectors.models import Document
from danswer.connectors.models import Section


class FreshDeskConnector(PollConnector):
    def __init__(self, batch_size: int = INDEX_BATCH_SIZE) -> None:
        self.batch_size = batch_size
        self.tickets = []

    def txn_link(self, tid: int) -> str:
       return f"https://{self.domain}.freshdesk.com/helpdesk/tickets/{tid}"
        
    def load_credentials(self, credentials: dict[str, str]) -> None:
        self.api_key = credentials.get("freshdesk_api_key")
        self.domain = credentials.get("freshdesk_domain")
        return None
    
    def fetch_tickets(self):
        freshdesk_url = f"https://{self.domain}.freshdesk.com/api/v2/tickets?include=description&per_page=30"
        response = requests.get(freshdesk_url, auth=(self.api_key, "x"))
        if response.status_code == 200:

            soup = BeautifulSoup(response.text, "html.parser")
            response_bs4 = soup.get_text()
            self.tickets = json.loads(response_bs4)

    
    def build_doc_sections_from_txn(self, ticket: dict) -> List[Section]:  
        keys_not_to_include = ["spam", "email_config_id", "association_type", 
                               "custom_fields", "associated_ticket_count", "tags", "description", 
                               "internal_agent_id", "internal_group_id", "nr_due_by", "nr_escalated"]
        return [Section(
             link = self.txn_link(int(ticket["id"])), 
             text = json.dumps({
                  key : value
                  for key, value in ticket.items()
                    if key not in keys_not_to_include & isinstance(value, str)
                }, default = str),
        )]
    
    

    def _process_tickets(self, start: datetime, end: datetime) -> GenerateDocumentsOutput:

            self.fetch_tickets()

            doc_batch: List[Document] = []
            ticket_keys_to_include = ["id", "status", "priority", "type", "is_overdue"]

            today = datetime.now()
            

            for ticket in self.tickets:
                    
                tid: int = int(ticket["id"])
            
                Section = self.build_doc_sections_from_txn(ticket)      

                #1. status mapping
                status_mapping = {
                    2: "Open",
                    3: "Pending, Awaiting Developer Fix",
                    4: "Resolved, Resolved",
                    5: "Closed",
                    6: "Waiting on Customer, Need Additional Information",
                    8: "Monitoring, Under Observation",
                    13: "Update/Upgrade Requested, Upgrade Initiated",
                    14: "Implementation Request, Implementation Request"
                }
                ticket["status"] = status_mapping.get(ticket["status"])

                #2. remove whitespace from description

                #3. check if ticket is overdue
                due_by = datetime.strptime(ticket["due_by"], "%Y-%m-%d %H:%M:%S")

                if due_by < today:
                    ticket["is_overdue"] = True
                else:
                    ticket["is_overdue"] = False
                
                doc = Document(
                    id=ticket["id"],
                    sections = Section,
                    source=DocumentSource.FRESHDESK,
                    semantic_identifier=ticket["subject"],
                    metadata={
                        key: value
                        for key, value in ticket.items()
                        if key in ticket_keys_to_include and value is not None
                    },
                )

                doc_batch.append(doc)

                if len(doc_batch) >= self.batch_size:
                    yield doc_batch
                    doc_batch = []

                if doc_batch:
                    yield doc_batch

    def poll_source(
            self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch
        ) -> GenerateDocumentsOutput:
        
            yield from self._process_tickets(start, end)


if __name__ == "__main__":
    import time
    import os
    from dotenv import load_dotenv

    load_dotenv()
    logger.setLevel(LOG_LVL_DEBUG)
    fd_connector = FreshDeskConnector()
    fd_connector.load_credentials(
        {
            "freshdesk_domain": os.getenv("FD_DOMAIN"),      
            "freshdesk_api_key": os.getenv("FD_API_KEY"),
        }
    )

    current = time.time()
    one_day_ago = current - (24 * 60 * 60)  # 1 days
    latest_docs = fd_connector.poll_source(one_day_ago, current)

    for doc in latest_docs:
        print(doc)

