FROM bitnami/spark:latest

# Créer un utilisateur 'sparkuser' avec home
RUN useradd -m -u 1000 sparkuser

# Donner les droits sur /opt/bitnami/spark/ivy (au cas où)
RUN mkdir -p /opt/bitnami/spark/ivy && chown -R sparkuser:sparkuser /opt/bitnami/spark/ivy

# Basculer vers l'utilisateur sparkuser
USER sparkuser

ENV HOME=/home/sparkuser

WORKDIR /opt/bitnami/spark
