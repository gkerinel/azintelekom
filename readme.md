AWS lambda fonksiyorunu kullanarak. AWS bucketımızdaki verileri Scality storage da bulunan bucket a günlük olarak aktarma işi için hazırlanmış python scriptidir.
Günlük gelen veri s3 bucketı tetiklemesi için eventbridge kullanıldı. 
SES üzerinde notifikasyon configure edildi.
lambda.py: asdailylog dosyasına o tarihteki silinen ve eklenen dosyaları yazdırmaktadir fakat her gün yeni bir txt dosyası olarak atar.
lambda2.py: Müşterinin isteği üzerinde bütün veriler tek bir dosyada toplanması istemiştir. Geçmişteki bütün txt dosyalarını içindeki verileri ve yeni veriyi buraya yazdırır.
