# eventzAPI
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v1.4%20adopted-ff69b4.svg)](code-of-conduct.md)

The eventzAPI supports a Python micro services infrastructure.
It employs publish and subscribe messaging using AMQP messaging (RabbitMQ) & Event Sourcing.
It provides support for Qt or tkinter GUIs.
A Librarian Client supports querying an indelible event store (Archive).

The API requires that a RabbitMQ server be accessible and if a master archive is needed, an Archivist and Librarian available through Docker Hub.
