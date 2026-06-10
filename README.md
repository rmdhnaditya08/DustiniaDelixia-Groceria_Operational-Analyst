# FP MCI
## DustiniaDelixia-Groceria_Operational-Analyst

## Studi Kasus Persona Operational
Head of Operational DustiniaDelixia Groceria mulai menerima semakin
banyak komplain terkait pengalaman pengiriman dari beberapa seller besar. Di saat
yang sama, tim Finance juga mulai menekan operasional untuk meningkatkan efisiensi
biaya distribusi. Kondisi ini membuat tim operasional membutuhkan pemahaman yang
lebih jelas mengenai performa pengiriman secara menyeluruh berdasarkan data aktual
di lapangan, bukan sekadar laporan agregat.
DustiniaDelixia Groceria memiliki sistem pelacakan yang merekam berbagai
timestamp penting dalam perjalanan setiap pesanan, mulai dari pesanan dibuat
pelanggan, pembayaran disetujui, barang diproses seller, hingga pesanan diterima
pelanggan. Data tersebut merepresentasikan keseluruhan alur operasional pengiriman
dalam skala yang sangat besar.
Perusahaan menyadari bahwa masih terdapat banyak pesanan yang tidak
berjalan sesuai ekspektasi pelanggan, namun belum memiliki pemetaan yang jelas
mengenai pola keterlambatan maupun kondisi operasional yang paling sering
mempengaruhi pengalaman pelanggan. Selain itu, belum diketahui apakah
permasalahan tersebut terjadi secara konsisten di seluruh wilayah dan seller atau
hanya muncul pada kondisi tertentu saja.
Data operasional yang dimiliki perusahaan diperkirakan masih menyimpan
banyak pola yang belum pernah dianalisis lebih dalam.

### Masalah Inti
1. Di mana bottleneck terjadi? — Seller atau kurir?
2. Apakah masalah konsisten di semua wilayah atau kondisi tertentu?
3. Seller mana yang paling bermasalah?

## Deskripsi Pipeline
Pipeline ini membangun sistem analitik **micro-batching** untuk dataset Orders dari REST API. Data ditarik secara periodik setiap 10 menit menggunakan Apache Airflow, diproses dengan Apache Spark, dimuat ke ClickHouse sebagai Data Warehouse, lalu divisualisasikan melalui dashboard Metabase.
