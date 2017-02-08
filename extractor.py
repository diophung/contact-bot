import re


class Extractor:
    def extract_details(self, from_msg):
        """
        rtype: email, phone
        """
        # email format
        email = re.search(r'[\w\.-]+@[\w\.-]+', from_msg)

        # US & VN mobile phone formats: 555-555-5555, 0988 123 456, 0165 123 4567
        phone = re.search(r"\(?\b[0-9][0-9]{2}\)?[-. ]?[0-9][0-9]{2}[-. ]?[0-9]{4}\b|(\d{4}[-. ]?\d{3}[-. ]?(\d{3,4}|\d{4}))|(\d{10})", from_msg)
        if email and phone and email.group(0) and phone.group(0):
            return email.group(0), phone.group(0)
        return "", ""
