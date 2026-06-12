Środowisko skonfigorowałem lokalnie i wykorzystałem JDK 17 i PySpark'a zainstalowanego przez pip. Na początku skryptu ustawiam zmienne środowiskowe JAVA_HOME i HADOOP_HOME w którym jest winutils.exe i hadoop.dll wymagane przez Sparka. Sesję utworzyłem używając SparkSession.builder. Za pomocą spark.version sprawdziłem poprawność 

<img width="754" height="46" alt="image" src="https://github.com/user-attachments/assets/53057446-2445-4322-a00a-8645c01d4573" />

źródłem danych strumieniowych są pliki CSV w data/input_stream. Każdy rekord zawiera czas zdarzenia, id, kategorie, wartość liczbową i status zdarzenia. Dane wczytałem za pomocą spark.readStream.csv z opcją header. Event_time zamieniłem z tekstu na timestamp, usunąłem rekordy z brakami za pomocą dropna oraz odrzuciłem rekordy z ujemną kwotą. df.isStreaming rzuciła True co potwierdziło że to strumieniowy charakter.

<img width="376" height="140" alt="image" src="https://github.com/user-attachments/assets/b7cb9d77-185c-47dd-8cf4-0a8e85647223" />

Na strumieniu wykonałem filtrowanie zdarzeń paid i obliczanie kolumny size (large jeżeli >= 100) lub small. Następnie wykonałem agregację według kategorii która oblicza count i sumę wartości. Wynik zapisywany jest do konsoli przez writeStream. Nowe pliki dodawane są automatycznie przez skrypt generator.py który działa w drugim terminalu co 5 sekund tworząc nowe rekordy. Spark automatycznie wykrywa kolejne pliki i przetwarzał je bez restartu programu co widać przez rosnące batche 

<img width="405" height="86" alt="image" src="https://github.com/user-attachments/assets/8256cc7e-921b-412e-a14d-98150585cf13" />


<img width="364" height="239" alt="image" src="https://github.com/user-attachments/assets/e46be0cc-b7f7-4f9f-96e0-e556a9b7a62c" />

Uruchomiłem app_zad4_5 z generatorem danych która agreguje strumień według czasu zdarzenia

<img width="571" height="460" alt="image" src="https://github.com/user-attachments/assets/48f67016-1a82-4aa6-bdc3-f59d9aa0142b" />

Równolegle uruchomione jest drugie zapytanie z oknem przesuwanym. To zdarzenie trafia do dwóch nakładających się okien, stąd więcej wierszy niż przy oknie stałym

<img width="737" height="439" alt="image" src="https://github.com/user-attachments/assets/25184e11-8877-4539-99ca-5c6bfd687cdc" />

Generator losowo dodaje zdarzenia opóźnione o 90 sekund i widać że zostały dodane do wcześniejszego okna

<img width="274" height="499" alt="image" src="https://github.com/user-attachments/assets/07c19435-2ab1-4f4e-9492-1c1052e99381" />

Dane opóźnione w granicy watermarku zostały doliczone do starego okna

Zatrzymałem aplikację i uruchomiłem ją ponownie bez usuwania folderu checkpointów. Pierwszy batch po restarcie nie zawiera starych danych co potwierdza że spark nie przetwarza jeszcze raz tych plików. Po zatrzymaniu przetwarzania wczytałem zapisane pliki Parquet przez read_results.py jako batchowy DataFrame

<img width="733" height="654" alt="image" src="https://github.com/user-attachments/assets/62e6915f-d916-40d5-a1ff-eace0f38b34d" />

Wnioski : 
Spark Structured Streaming pozwala przetwarzać napływające pliki CSV w sposób ciągły bez restartu aplikacji. Agregacje w oknach czasowych poprawnie obsługują opóźnione dane a checkpointing zapewnia odporność na restart bez ponownego przetwarzania danych




