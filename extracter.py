def extract_details(from_msg):
    """
    rtype: email, phone
    """
    email = re.search(r'[\w\.-]+@[\w\.-]+', from_msg)
    phone = re.search(r"\(?\b[2-9][0-9]{2}\)?[-. ]?[2-9][0-9]{2}[-. ]?[0-9]{4}\b", from_msg)
    if email and phone and email.group(0) and phone.group(0):
        return email.group(0), phone.group(0)
    return "<error>","<error>"