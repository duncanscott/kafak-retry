# kafka-error-retry
Route kafka messages that fail processing to an error queue.  Messages
in the error queue are retried on a regular schedule.  If a message
is not successfully processed within a specified time limit, it will
be routed to a dead letter queue.

## example use case
A microservice is listening for events on particular topic
and sending out emails to stakeholders of projects identified in certain events.
To send emails, it needs to find stakeholders associated with a project
and obtain their emails.  A legacy contacts system throws errors for some information requests: perhaps
no manager is assigned to the project, perhaps an email address is missing.  Events
for which emails could not be sent are routed to an error topic and re-tried at intervals over a few days.
Some messages eventually succeed: a manager was assigned to a project, a missing email
address was provided.   After three days any messages for which emails still have not been sent are
routed to a dead-letter-queue where they are attended to by support staff.

## configuring topics

