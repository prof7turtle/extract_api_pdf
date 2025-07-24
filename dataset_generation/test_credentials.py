import json
from adobe.pdfservices.operation.auth.service_principal_credentials import ServicePrincipalCredentials
from adobe.pdfservices.operation.pdf_services import PDFServices

def test_adobe_credentials():
    try:
        # Load your downloaded JSON credentials
        with open("pdfservices-api-credentials.json", 'r') as f:
            creds = json.load(f)
        
        credentials = ServicePrincipalCredentials(
            client_id=creds["client_credentials"]["client_id"],
            client_secret=creds["client_credentials"]["client_secret"]
        )
        
        pdf_services = PDFServices(credentials=credentials)
        print("✅ Adobe PDF Services API credentials are valid!")
        return True
        
    except Exception as e:
        print(f"❌ Credential error: {e}")
        return False

if __name__ == "__main__":
    test_adobe_credentials()
