import cryptography.fernet as f
import base64

class Obfuscator:
    """This class is used to obfuscate/deobfuscate sensitive data such as passwords that must not be shown in cleartext.
    """

    #The class attribute ___fernetK can be protected to some degree by converting the content
    #into base64 --> any data (including binary) in a text version.
    #After this conversion, the key is NOT directly readable in RAM
    __fernetK:bytes = b'UUJTeDdOY1liYkxQZWVPQmlTNjlZOWFmYjhmNV8wZUxQVTY2U1A2cmZvTT0='

    def __init__(self:object)->None:
        #We need to decode the base64 in order to use it
        self._obfuscator:f.Fernet = f.Fernet(base64.b64decode(Obfuscator.__fernetK))

    def obfuscate(self:object, clearText:str)->str:
        #As the Fernet class expects values given in byte format we use encode() to convert the string value to byte
        #encrypt() in Fernet returns bytes which are then converted into a str using the decode function
        return (self._obfuscator.encrypt(clearText.encode())).decode()

    def deObfuscate(self:object, encryptedText:str)->str:
        #As the Fernet class expects values given in byte format we use encode() to convert the string value to byte
        #decrypt() in Fernet returns bytes which can be converted into a str using the decode function     
        return (self._obfuscator.decrypt(encryptedText.encode())).decode()
