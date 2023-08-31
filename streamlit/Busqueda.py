from json import dumps

from kafka import KafkaProducer
from kafka import KafkaConsumer

import streamlit as st


st.title('Bienvenido a ElMercado.com')
st.header('Estás en la seccion de busqueda')
st.subheader('Busca aqui el artículo que quieras comprar')
articulo= st.text_input("Describe tu articulo y pulsa enter")

my_producer = KafkaProducer(
    bootstrap_servers = ['workernode1:9092'],
    value_serializer = lambda x:dumps(x).encode('utf-8')
    )

my_producer.send('streaming-query', value = articulo)

# generating the Kafka Consumer
my_consumer = KafkaConsumer(
    'output',
     bootstrap_servers = ['workernode1:9092'],
     auto_offset_reset = 'latest',
     enable_auto_commit = False,
     # group_id = 'my-group',
     # value_deserializer = lambda x : loads(x.decode('utf-8'))
     )

st.text("resultados para '" + articulo + "'")
for message in my_consumer:
	mensaje = message.value.decode('utf-8').strip("'")
	st.text((mensaje))