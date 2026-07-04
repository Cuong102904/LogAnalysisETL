FROM trinodb/trino:455

USER root

RUN microdnf install -y gettext     && microdnf clean all

COPY etc /etc/trino
COPY entrypoint.sh /opt/trino/entrypoint.sh

RUN chmod +x /opt/trino/entrypoint.sh

ENTRYPOINT ["/opt/trino/entrypoint.sh"]
