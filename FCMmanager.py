import firebase_admin
from firebase_admin import credentials, messaging

cred = credentials.Certificate("/home/rasam-user/Alarm/serviceAccountKey.json")
firebase_admin.initialize_app(cred)


def sendNotification(title, msg, registration_token, dataObject=None):
    message = messaging.MulticastMessage(
        notification=messaging.Notification(
            title=title,
            body=msg
        ),
        data=dataObject,
        tokens=registration_token,
    )

    response = messaging.send_each_for_multicast(message)
    print('Successfully sent message:', response)
