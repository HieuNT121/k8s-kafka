# Phase 5: Kafka Data Model và thực hành Producer/Consumer

Ở Phase 4, Kafka cluster đã có persistent storage và topic `orders`. Phase này đưa dữ liệu thực sự vào Kafka và đọc dữ liệu ra bằng Kafka CLI. Trọng tâm là mối quan hệ:

```text
Topic -> Partition -> Replica -> Leader/Follower -> ISR -> Offset
                                      |
                         Producer -> Kafka -> Consumer Group
```

Project hiện tại có topic:

```yaml
partitions: 3
replicas: 3
```

Điều đó có nghĩa là có 3 partition và mỗi partition có 3 replica, tổng cộng 9 partition replica trong cluster.

## 1. Mục tiêu học tập

Sau Phase 5, bạn có thể:

- Phân biệt topic, partition, replica, leader, follower và ISR.
- Hiểu offset là vị trí theo từng partition, không phải global offset.
- Mô tả producer chọn partition và gửi record đến leader.
- Dùng Kafka console producer để gửi message.
- Dùng Kafka console consumer để đọc message.
- Hiểu consumer group, partition assignment và giới hạn scale theo số partition.
- Kiểm tra committed offset, log-end offset và consumer lag.
- Replay dữ liệu bằng consumer group mới.
- Giải thích tác dụng của các Pod trong hệ thống Kafka Kubernetes hiện tại.

## 2. Các Pod trong hệ thống Kafka này có tác dụng gì?

Kiểm tra Pod:

```bash
kubectl get pods -n kafka -o wide
```

Môi trường hiện tại có các nhóm Pod sau:

```text
strimzi-cluster-operator-...       1/1
kafka-cluster-kafka-pool-0         1/1
kafka-cluster-kafka-pool-1         1/1
kafka-cluster-kafka-pool-2         1/1
kafka-cluster-entity-operator-...  2/2
```

### 2.1. Strimzi Cluster Operator Pod

```text
strimzi-cluster-operator-...
```

Pod này là control plane của Strimzi. Nó không lưu hoặc xử lý message business như `order-001`. Nó theo dõi Kafka CR, KafkaNodePool và KafkaTopic rồi reconcile thành resource Kubernetes thực tế.

```text
Kafka CR / KafkaNodePool / KafkaTopic
              |
              v
Strimzi Operator Pod
              |
              v
Kafka Pods, Services, Config, Storage
```

Operator Pod chịu trách nhiệm tạo, cập nhật hoặc phục hồi các resource theo desired state. Nếu Operator chết, Kafka Pod có thể tiếp tục xử lý dữ liệu trong một thời gian, nhưng các thay đổi mới và một số thao tác quản lý sẽ không được reconcile cho đến khi Operator hoạt động lại.

Kiểm tra:

```bash
kubectl get deployment strimzi-cluster-operator -n kafka
kubectl logs deployment/strimzi-cluster-operator -n kafka --tail=50
```

### 2.2. Ba Kafka node Pod

```text
kafka-cluster-kafka-pool-0
kafka-cluster-kafka-pool-1
kafka-cluster-kafka-pool-2
```

Đây là data plane chính của Kafka. Mỗi Pod chạy một Kafka node với hai role:

```text
Kafka node
  +-- broker
  +-- controller
```

Broker role:

- Nhận request từ producer.
- Phục vụ request từ consumer.
- Lưu partition log trên volume tương ứng.
- Giữ leader hoặc follower replica của partition.

Controller role:

- Tham gia KRaft controller quorum.
- Quản lý metadata cluster.
- Điều phối thay đổi topic, partition, replica và leader.
- Tham gia leader election khi controller leader gặp lỗi.

Quan hệ với storage:

```text
kafka-pool-0 -> PVC/PV riêng
kafka-pool-1 -> PVC/PV riêng
kafka-pool-2 -> PVC/PV riêng
```

Không nên xóa trực tiếp Kafka Pod để thay đổi cấu hình. Hãy thay đổi `Kafka` hoặc `KafkaNodePool` CR và để Strimzi reconcile.

### 2.3. Entity Operator Pod

```text
kafka-cluster-entity-operator-...
READY: 2/2
```

Pod này có hai container vì manifest Kafka bật:

```yaml
entityOperator:
  topicOperator: {}
  userOperator: {}
```

Topic Operator container:

- Theo dõi `KafkaTopic` resource.
- Đồng bộ `KafkaTopic` với Kafka topic thật.
- Cho phép tạo hoặc thay đổi topic bằng Kubernetes YAML.

User Operator container:

- Theo dõi `KafkaUser` resource.
- Tạo và quản lý Kafka user, Secret và cấu hình authentication/authorization theo manifest.

`2/2` nghĩa là hai container trong cùng Entity Operator Pod đều ready. Entity Operator không phải Kafka broker; nó là lớp quản lý entity Kafka thông qua Kubernetes.

### 2.4. Pod client ở Phase này

Hiện tại project chưa có producer/consumer application Pod riêng. Chúng ta chạy Kafka CLI bên trong một Kafka Pod bằng `kubectl exec`:

```text
Mac terminal
    |
    +-- kubectl exec
            |
            v
       Kafka node Pod
            |
            v
       Kafka CLI client
```

Kafka CLI process chỉ là client tạm thời chạy bên trong container; nó không biến Kafka Pod thành consumer service riêng. Ở các phase sau có thể tạo Deployment/Pod riêng cho producer và consumer.

### 2.5. Tóm tắt vai trò

| Pod | Vai trò | Có xử lý business message? |
| --- | --- | --- |
| Strimzi Cluster Operator | Reconcile Kubernetes/Kafka resources | Không |
| Kafka node 0, 1, 2 | Broker + KRaft controller | Có |
| Entity Operator | Quản lý topic/user qua CRD | Không trực tiếp |
| Client Pod nếu tạo | Producer/consumer application | Có, thông qua Kafka API |

Pod là đơn vị chạy container. Topic, partition, replica, offset và consumer group là các khái niệm Kafka nằm bên trong hoặc được Kafka quản lý; chúng không phải là các Pod riêng biệt.

## 3. Topic, partition và replica

### 3.1. Topic

Topic là logical stream của record:

```text
orders
payments
notifications
```

Topic `orders` có thể chứa các event như `OrderCreated`, `OrderPaid` và `OrderShipped`.

Kiểm tra topic:

```bash
kubectl get kafkatopic -n kafka
kubectl describe kafkatopic orders -n kafka
```

### 3.2. Partition

Partition là ordered append-only log:

```text
orders / partition 0
  offset 0 -> Order A
  offset 1 -> Order B
  offset 2 -> Order C
```

Partition là đơn vị chính để Kafka lưu trữ và xử lý song song. Kafka chỉ đảm bảo thứ tự trong một partition, không đảm bảo thứ tự toàn topic.

Ba partition hiện tại:

```text
orders
  +-- partition 0
  +-- partition 1
  +-- partition 2
```

### 3.3. Replica

`replicas: 3` trong `KafkaTopic` nghĩa là mỗi partition có ba replica:

```text
partition 0 -> broker 0, broker 1, broker 2
partition 1 -> broker 1, broker 2, broker 0
partition 2 -> broker 2, broker 0, broker 1
```

Replication factor 3 không có nghĩa topic có 3 partition. Trong project:

```text
partitions = 3
replication factor = 3
partition replicas = 3 x 3 = 9
```

## 4. Leader, follower và ISR

Mỗi partition có một leader và các follower:

```text
partition 0
  +-- broker 0: leader
  +-- broker 1: follower
  +-- broker 2: follower
```

Producer gửi record đến leader của partition. Follower replicate dữ liệu từ leader.

ISR là In-Sync Replicas, tức các replica đang đáp ứng điều kiện đồng bộ của Kafka:

```text
Leader: broker 0
ISR:    broker 0, broker 1, broker 2
```

Nếu broker 2 chậm hoặc lỗi:

```text
ISR: broker 0, broker 1
```

ISR không phải danh sách tất cả replica từng tồn tại; nó phản ánh các replica hiện được coi là đồng bộ. Khi leader lỗi, Kafka có thể bầu leader mới từ replica phù hợp, tùy trạng thái ISR và cấu hình cluster.

## 5. Offset

Offset là vị trí của record trong một partition:

```text
partition 0
  offset 0 -> order-001
  offset 1 -> order-002
  offset 2 -> order-003
```

Offset không phải global:

```text
partition 0: 0, 1, 2
partition 1: 0, 1
partition 2: 0, 1, 2
```

Mỗi partition có dãy offset riêng. Consumer group lưu tiến độ đọc riêng cho từng partition.

## 6. Kiểm tra Kafka CLI

Lấy tên Kafka Pod:

```bash
kubectl get pods -n kafka
```

Chọn một Kafka node Pod, không chọn Operator hoặc Entity Operator:

```bash
kubectl exec -it kafka-cluster-kafka-pool-0 -n kafka -- bash
```

Kiểm tra Kafka binary:

```bash
ls /opt/kafka/bin
```

Các command chính:

```text
kafka-topics.sh
kafka-console-producer.sh
kafka-console-consumer.sh
kafka-consumer-groups.sh
```

Tên Pod có thể khác theo Strimzi version. Có thể chạy command mà không mở shell:

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
```

## 7. Describe topic và đọc partition assignment

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
```

Output thường có dạng:

```text
Topic: orders  PartitionCount: 3  ReplicationFactor: 3
Topic: orders  Partition: 0  Leader: 0  Replicas: 0,1,2  Isr: 0,1,2
Topic: orders  Partition: 1  Leader: 1  Replicas: 1,2,0  Isr: 1,2,0
Topic: orders  Partition: 2  Leader: 2  Replicas: 2,0,1  Isr: 2,0,1
```

Các con số thực tế có thể khác. Đọc một dòng như sau:

```text
Partition: 0
Leader: 0
Replicas: 0,1,2
Isr: 0,1,2
```

Nghĩa là broker 0 nhận request chính cho partition 0; ba broker đều có replica và hiện đều nằm trong ISR.

## 8. Producer và partitioner

Producer là client gửi record vào Kafka. Console producer mô phỏng một producer đơn giản:

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- bash

/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

Nhập từng dòng:

```text
order-001
order-002
order-003
```

Luồng conceptually:

```text
Console producer
      |
      v
Kafka producer client
      |
      v
Partitioner chọn partition
      |
      v
Partition leader
      |
      v
Follower replicas
```

Nếu record có key, partitioner thường dùng key để xác định partition:

```text
hash(key) -> partition
```

Các event cùng `orderId` có thể dùng cùng key để giữ thứ tự trong cùng partition. Console producer không có key khi dùng command đơn giản, vì vậy không nên kỳ vọng message lần lượt đi vào P0, P1, P2.

## 9. Consumer và Consumer Group

Consumer đọc record từ Kafka. Consumer group là tập consumer phối hợp với nhau để đọc topic.

Với ba partition và hai consumer trong cùng group:

```text
orders
  +-- P0 -> Consumer 1
  +-- P1 -> Consumer 2
  +-- P2 -> Consumer 1
```

Một partition tại một thời điểm chỉ được assign cho một consumer trong cùng group. Vì vậy với 3 partition, tối đa 3 consumer trong group có thể đồng thời có partition để xử lý; consumer thứ 4 có thể idle.

Hai group khác nhau đọc độc lập:

```text
orders
  +-- order-processing group
  +-- analytics group
```

Mỗi group có offset riêng. Đây là điểm khác với queue nơi record thường được phân phối giữa các consumer dùng chung một tiến độ.

## 10. Consume message

Mở terminal thứ hai và chạy consumer không group để quan sát nhanh:

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning
```

`--from-beginning` yêu cầu đọc từ đầu khi consumer chưa có committed offset phù hợp. Giữ consumer chạy rồi quay lại producer gửi:

```text
order-004
order-005
```

Consumer sẽ nhận record mới.

Để tạo consumer group thực sự:

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group order-processing \
  --from-beginning
```

Dừng consumer bằng `Ctrl+C` khi cần. Consumer group offset được lưu trong internal topic:

```text
__consumer_offsets
```

Không nên xem `--from-beginning` là lệnh luôn reset group về offset 0. Nếu group đã có committed offset, behavior còn phụ thuộc offset hiện có và `auto.offset.reset`.

## 11. Kiểm tra Consumer Group và lag

Liệt kê group:

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Describe group:

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-processing
```

Output thường có các cột:

```text
GROUP  TOPIC  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
```

Ý nghĩa:

- `CURRENT-OFFSET`: tiến độ đã commit của group trên partition.
- `LOG-END-OFFSET`: vị trí cuối hiện có của partition.
- `LAG`: lượng dữ liệu group chưa bắt kịp.

Công thức khái niệm:

```text
LAG = LOG-END-OFFSET - CURRENT-OFFSET
```

Lag bằng 0 thường nghĩa consumer đang theo kịp tại thời điểm kiểm tra. Lag tăng liên tục là dấu hiệu consumer xử lý chậm hơn tốc độ producer hoặc downstream đang nghẽn.

## 12. Thực hành nhiều consumer

### Consumer 1

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group order-processing
```

### Consumer 2

Chạy cùng command ở terminal khác, vẫn dùng:

```text
--group order-processing
```

Gửi thêm message từ producer:

```text
order-006
order-007
order-008
order-009
order-010
```

Hai consumer trong cùng group sẽ chia partition, không phải mỗi consumer nhận toàn bộ message. Assignment cụ thể phụ thuộc group coordinator và assignment strategy.

Tạo group thứ hai:

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group analytics \
  --from-beginning
```

Group `analytics` có thể đọc lại cùng các record mà `order-processing` đã đọc, vì hai group có committed offset độc lập.

## 13. Replay và ordering

Consumer group mới có thể đọc dữ liệu lịch sử bằng `--from-beginning` nếu chưa có committed offset và retention vẫn còn dữ liệu. Đây là cơ chế hữu ích để xây dựng lại state hoặc bootstrap một application mới.

Kafka đảm bảo ordering trong cùng partition:

```text
P0:
  0 OrderCreated
  1 OrderPaid
  2 OrderShipped
```

Kafka không đảm bảo ordering giữa P0 và P1. Nếu business yêu cầu event của cùng một order theo đúng thứ tự, producer nên dùng key ổn định như `orderId` để các event của order đó đi vào cùng partition.

## 14. Luồng đầy đủ của một record

Ví dụ record `OrderCreated`, key `orderId:1001`:

```text
1. Producer tạo record
2. Partitioner dùng key để chọn partition
3. Producer gửi record tới partition leader
4. Follower replicas replicate record
5. Kafka ghi record với offset trong partition
6. Consumer group nhận partition assignment
7. Consumer đọc record
8. Consumer commit offset
9. Kafka lưu tiến độ group trong __consumer_offsets
```

Tóm tắt kiến trúc:

```text
Producer
   |
   v
orders topic
   +-- P0 -- replicas -- leader/followers -- ISR
   +-- P1 -- replicas -- leader/followers -- ISR
   +-- P2 -- replicas -- leader/followers -- ISR
   |
   v
Consumer group
   +-- Consumer 1
   +-- Consumer 2
```

## 15. Troubleshooting

### Không tìm thấy Kafka CLI

Đảm bảo đã exec vào Kafka node Pod, không phải Strimzi Operator hoặc Entity Operator:

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- ls /opt/kafka/bin
```

### `Connection refused` tới `localhost:9092`

Kiểm tra Pod đang chọn là Kafka Pod và listener nội bộ đã sẵn sàng:

```bash
kubectl get pods -n kafka
kubectl get kafka kafka-cluster -n kafka
kubectl get svc -n kafka
```

Nếu chạy CLI trong Kafka Pod, `localhost:9092` thường phù hợp cho listener nội bộ trong lab. Client bên ngoài Pod cần dùng địa chỉ listener/service phù hợp, không mặc định dùng localhost.

### Topic không tồn tại

```bash
kubectl get kafkatopic orders -n kafka
kubectl describe kafkatopic orders -n kafka
kubectl logs deployment/strimzi-cluster-operator -n kafka --tail=100
```

Kiểm tra Topic Operator trong Entity Operator có ready và topic label có trỏ tới `kafka-cluster`.

### Consumer không thấy message cũ

Kiểm tra group đã có committed offset chưa. Tạo group mới cho bài replay hoặc kiểm tra `auto.offset.reset`:

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

`--from-beginning` không tự động ghi đè committed offset hiện tại.

### Consumer group không có assignment

Kiểm tra số consumer, số partition và trạng thái group:

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-processing
```

Consumer có thể đang rebalancing hoặc group có nhiều consumer hơn số partition.

### ISR thiếu broker

Describe topic:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
```

Sau đó kiểm tra Pod, Events, storage và log Kafka. ISR thiếu có thể do broker restart, resource pressure, network hoặc replication chưa bắt kịp.

## 16. Bài thực hành tổng hợp

### Bước 1: Kiểm tra infrastructure

```bash
kubectl get pods -n kafka -o wide
kubectl get kafkatopic -n kafka
kubectl get pvc -n kafka
```

### Bước 2: Describe topic

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic orders
```

### Bước 3: Produce

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

Gửi 5 record mẫu rồi dừng bằng `Ctrl+C`.

### Bước 4: Consume bằng group

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group order-processing \
  --from-beginning
```

### Bước 5: Kiểm tra lag

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group order-processing
```

### Bước 6: Replay bằng group mới

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group analytics \
  --from-beginning
```

## 17. Checklist hoàn thành Phase 5

- [ ] Hiểu topic là logical stream.
- [ ] Hiểu partition là ordered log và đơn vị parallelism.
- [ ] Phân biệt 3 partition với replication factor 3.
- [ ] Hiểu leader, follower và ISR.
- [ ] Hiểu offset theo từng partition.
- [ ] Biết Strimzi Operator Pod quản lý resource, không xử lý business message.
- [ ] Biết Kafka node Pod xử lý broker và KRaft controller.
- [ ] Biết Entity Operator Pod quản lý KafkaTopic/KafkaUser.
- [ ] Đã describe topic và đọc leader/replicas/ISR.
- [ ] Đã produce message vào topic `orders`.
- [ ] Đã consume message từ topic.
- [ ] Đã tạo và kiểm tra consumer group.
- [ ] Đã đọc current offset, log-end offset và lag.
- [ ] Đã chạy hai consumer trong cùng group.
- [ ] Đã tạo group thứ hai để replay dữ liệu.
- [ ] Hiểu ordering chỉ được đảm bảo trong một partition.

## 18. Kết nối sang Phase 6

Trong Phase 5, client chạy bằng `kubectl exec` bên trong Kafka Pod. Đây là cách học CLI thuận tiện nhưng không phải kiến trúc application hoàn chỉnh.

Phase 6 sẽ tập trung vào Kafka networking và client connectivity:

```text
Application trong Kubernetes -> Kafka Service -> Kafka broker
Application trên Mac        -> external listener -> Kafka broker
```

Khi đó cần hiểu `bootstrap.servers`, advertised listeners, internal/external listener, DNS service discovery và giới hạn của `kubectl port-forward` với Kafka.