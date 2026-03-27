"""
CRM Factory - Multi-CRM Support
Returns the right CRM client module based on the --crm argument.

Supported CRMs:
  hubspot  (default) - HubSpot CRM via private app token
  attio              - Attio CRM via API token

Both clients expose the same public interface:
  create_deal(score_result, analysis, metadata, dry_run) -> dict | None
  find_or_create_company(name, industry, domain) -> str | None
  find_or_create_contact(name, email, company_name) -> str | None
"""

SUPPORTED_CRMS = ("hubspot", "attio")


def get_client(crm: str = "hubspot"):
    """
    Return the CRM client module for the given CRM name.

    Usage:
        from crm import get_client
        client = get_client("hubspot")
        client.create_deal(score_result, analysis, metadata)
    """
    crm = crm.lower().strip()
    if crm == "attio":
        import attio_client
        return attio_client
    elif crm == "hubspot":
        import hubspot_client
        return hubspot_client
    else:
        raise ValueError(
            f"Unsupported CRM: '{crm}'. Choose from: {', '.join(SUPPORTED_CRMS)}"
        )
