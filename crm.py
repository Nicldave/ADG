"""
CRM Factory - Multi-CRM Support
Returns the right CRM client module based on the --crm argument.

Supported CRMs:
  hubspot     (default) - HubSpot CRM via private app token
  attio                 - Attio CRM via API token
  salesforce            - Salesforce via Connected App (instance_url|access_token)

All clients expose the same public interface:
  create_deal(score_result, analysis, metadata, dry_run) -> dict | None
  find_or_create_company(name, industry, domain) -> str | None
  find_or_create_contact(name, email, company_name) -> str | None
  find_deal_by_company(company_name) -> dict | None
  update_deal_stage(deal_id, stage) -> dict | None
  query_deals_by_stage(stages, limit) -> list[dict]
"""

SUPPORTED_CRMS = ("hubspot", "attio", "salesforce", "pipedrive", "close", "copper", "zoho", "freshsales")


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
    elif crm == "salesforce":
        import salesforce_client
        return salesforce_client
    elif crm == "pipedrive":
        import pipedrive_client
        return pipedrive_client
    elif crm == "close":
        import close_client
        return close_client
    elif crm == "copper":
        import copper_client
        return copper_client
    elif crm == "zoho":
        import zoho_client
        return zoho_client
    elif crm == "freshsales":
        import freshsales_client
        return freshsales_client
    else:
        raise ValueError(
            f"Unsupported CRM: '{crm}'. Choose from: {', '.join(SUPPORTED_CRMS)}"
        )
