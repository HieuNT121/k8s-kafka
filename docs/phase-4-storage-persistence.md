# Phase 4: Kafka Storage và Persistence trên Kubernetes

Phase này chuyển Kafka từ trạng thái chỉ chạy được sang trạng thái có dữ liệu bền vững qua vòng đời của Pod. Chúng ta tìm hiểu cách Kafka lưu partition log, cách Kubernetes cấp phát storage bằng `StorageClass`, `PersistentVolume` và `PersistentVolumeClaim`, rồi kiểm tra PVC sau khi Kafka Pod được restart hoặc recreate.

> Cấu hình hiện tại là lab trên Minikube. StorageClass `standard` và local storage của Minikube không đại diện cho storage production. Không dùng cấu hình này để đánh giá high availability hoặc disaster recovery.

## 1. Kết quả mong đợi

Sau Phase 4, kiến trúc có dạng:

```text
Kafka cluster
  +-- Kafka node 0 -> PVC 0 -> PV 0
  +-- Kafka node 1 -> PVC 1 -> PV 1
  +-- Kafka node 2 -> PVC 2 -> PV 2
```

Mỗi Kafka node có storage claim riêng. Khi một Pod bị xóa:

```text
Pod cũ bị xóa
       |
       v
Pod mới được tạo lại
       |
       v
PVC cũ được mount lại
       |
       v
Dữ liệu trên volume tiếp tục tồn tại
```

Phase này không chứng minh mọi loại failure đều an toàn. Mất toàn bộ Minikube node hoặc xóa PVC vẫn có thể làm mất dữ liệu lab.

## 2. Mục tiêu học tập

Sau phase này, bạn có thể:

- Giải thích Kafka lưu message trong partition log như thế nào.
- Phân biệt ephemeral storage và persistent storage.
- Phân biệt PV, PVC và StorageClass.
- Mô tả dynamic provisioning.
- Cấu hình persistent storage trong `KafkaNodePool`.
- Kiểm tra PVC, PV, storage class và volume mount.
- Tạo topic có partition và replication factor.
- Phân biệt persistence, replication và KRaft consensus.
- Kiểm tra PVC sau khi xóa một Kafka Pod.
- Chẩn đoán PVC `Pending` và Kafka Pod chưa mount được volume.

## 3. Kafka lưu dữ liệu ở đâu?

Kafka lưu record trong các partition log. Mỗi partition là một ordered log được chia thành nhiều log segment cùng các index phục vụ việc đọc:

```text
Kafka broker
  +-- data directory
        +-- orders-0
        |     +-- log segments
        |     +-- offset index
        |     +-- time index
        +-- payments-0
              +-- log segments
              +-- offset index
              +-- time index
```

Ví dụ dữ liệu trong partition:

```text
orders-0
  offset 0 -> Order A
  offset 1 -> Order B
  offset 2 -> Order C
```

Các file log và index cần nằm trên filesystem mà Kafka có thể tiếp tục sử dụng sau khi container hoặc Pod được thay thế. Vì vậy Kafka là stateful workload, khác với một ứng dụng stateless có thể tạo Pod mới mà không cần identity hoặc data directory cũ.

## 4. Ephemeral và persistent storage

### 4.1. Ephemeral storage

Với:

```yaml
storage:
  type: ephemeral
```

data gắn với lifecycle của Pod hoặc volume tạm thời do workload sử dụng:

```text
Kafka Pod -> ephemeral volume -> Pod bị thay thế -> data có thể mất
```

Ephemeral phù hợp để khởi động lab nhanh, kiểm tra cấu hình hoặc học KRaft khi chưa cần giữ dữ liệu. Không dùng nó cho Kafka production nếu chưa có một cơ chế durability khác được thiết kế và kiểm chứng.

### 4.2. Persistent storage

Với:

```yaml
storage:
  type: persistent-claim
  size: 10Gi
```

Strimzi yêu cầu Kubernetes tạo claim cho Kafka node:

```text
Kafka node -> PVC -> PV -> storage backend
```

Pod có thể bị thay thế nhưng PVC vẫn là resource độc lập với Pod. Pod mới có thể mount lại claim và đọc data directory cũ, tùy lifecycle, storage backend và trạng thái Kafka.

## 5. PV, PVC và StorageClass

### 5.1. PersistentVolume

PV là resource đại diện cho một volume storage mà cluster có thể cấp cho workload:

```text
PV
  capacity: 10Gi
  access mode: RWO
  storage backend
```

PV không phải request của ứng dụng. Nó là phần storage được cung cấp trong cluster.

### 5.2. PersistentVolumeClaim

PVC là request của ứng dụng hoặc controller:

```yaml
resources:
  requests:
    storage: 10Gi
```

PVC nói rằng workload cần một volume có capacity, access mode và StorageClass phù hợp. Khi claim được gắn với PV, status thường là `Bound`.

### 5.3. StorageClass

StorageClass mô tả provisioner và policy dùng để tạo storage. Trên Minikube thường có class `standard`, nhưng tên và provisioner có thể khác theo version hoặc cấu hình:

```bash
kubectl get storageclass
kubectl get storageclass -o yaml
```

Không nên giả định `standard` luôn tồn tại trên mọi cluster.

## 6. Dynamic provisioning

Kubernetes có thể tự tạo PV khi PVC được tạo:

```text
PVC yêu cầu 10Gi
        |
        v
StorageClass
        |
        v
Dynamic provisioner
        |
        v
PV được tạo và bind
```

Với Strimzi, người dùng khai báo storage trong `KafkaNodePool`; Operator tạo các resource phù hợp cho Kafka node. Không cần tự viết một PVC dùng chung cho cả ba broker.

## 7. Kiểm tra storage trước khi thay đổi

Chạy các lệnh sau từ thư mục project:

```bash
kubectl get storageclass
kubectl get pv
kubectl get pvc -n kafka
kubectl get kafkanodepool -n kafka
kubectl get kafka -n kafka
```

Diễn giải:

- Chưa có PVC có thể là bình thường nếu Kafka CR chưa hợp lệ hoặc NodePool đang dùng `ephemeral`.
- Kafka CR phải được Operator reconcile thành công trước khi Kafka node và PVC xuất hiện.
- Nếu Kafka CR đang `NotReady`, sửa lỗi Kafka/Strimzi trước khi debug PVC.

Kiểm tra version và trạng thái Kafka trước khi kết luận storage có lỗi:

```bash
kubectl describe kafka kafka-cluster -n kafka
kubectl get pods -n kafka
kubectl get events -n kafka --sort-by=.lastTimestamp
```

## 8. Cấu hình persistent storage cho KafkaNodePool

Manifest hiện tại:

```text
infrastructure/kafka/kafka-node-pool.yaml
```

Phần storage:

```yaml
storage:
  type: persistent-claim
  size: 10Gi
  class: standard
```

Ý nghĩa:

- `persistent-claim`: dùng PVC thay vì volume ephemeral.
- `size: 10Gi`: dung lượng yêu cầu cho mỗi Kafka node claim.
- `class: standard`: sử dụng StorageClass có tên `standard`.

Với ba replica, mục tiêu là mỗi node có claim riêng:

```text
Kafka node 0 -> PVC 0 -> PV 0
Kafka node 1 -> PVC 1 -> PV 1
Kafka node 2 -> PVC 2 -> PV 2
```

Không nên coi đây là một PVC duy nhất có ba consumer. Kafka node cần storage identity riêng vì mỗi node giữ các partition replica khác nhau.

Kiểm tra StorageClass trước khi apply:

```bash
kubectl get storageclass standard
```

Nếu class không tồn tại, dùng tên class thực tế của cluster hoặc bỏ trường `class` để sử dụng default StorageClass sau khi đã xác nhận default phù hợp.

## 9. Apply và kiểm tra PVC

Validate manifest:

```bash
kubectl apply --dry-run=client \
  -f infrastructure/kafka/kafka-node-pool.yaml
```

Apply NodePool:

```bash
kubectl apply \
  -f infrastructure/kafka/kafka-node-pool.yaml
```

Kiểm tra PVC:

```bash
kubectl get pvc -n kafka
kubectl get pvc -n kafka -o wide
```

Kết quả mong đợi sau khi Kafka cluster reconcile thành công là các claim tương ứng với Kafka node, thường có status:

```text
STATUS: Bound
```

Tên claim phụ thuộc Strimzi version. Không hard-code tên PVC trong script nếu chưa kiểm tra output thực tế.

Kiểm tra PV:

```bash
kubectl get pv
```

Đối chiếu claim và volume:

```text
PVC status = Bound
PVC volume = một PV cụ thể
PV claim    = kafka/<pvc-name>
```

Xem chi tiết một claim:

```bash
kubectl describe pvc <pvc-name> -n kafka
```

Chú ý `StorageClass`, `Capacity`, `Access Modes`, `Volume`, `Status` và `Events`.

## 10. Kiểm tra Pod mount volume

Sau khi PVC được bind:

```bash
kubectl get pods -n kafka -o wide
kubectl describe pod <kafka-pod-name> -n kafka
```

Trong output, kiểm tra hai phần:

```text
Volumes:
Mounts:
```

Mục tiêu là thấy Kafka Pod mount volume được tạo từ claim tương ứng. PVC `Bound` một mình chưa đủ chứng minh Kafka đang dùng đúng volume; cần kiểm tra Pod spec và trạng thái mount.

Kiểm tra volume từ Pod spec:

```bash
kubectl get pod <kafka-pod-name> -n kafka -o yaml
```

Không sửa trực tiếp Pod do Strimzi tạo. Thay đổi cấu hình storage trong `KafkaNodePool` rồi để Operator reconcile.

## 11. Tạo topic để chuẩn bị kiểm tra dữ liệu

Manifest hiện tại:

```text
infrastructure/kafka/kafka-topic.yaml
```

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: orders
  namespace: kafka
  labels:
    strimzi.io/cluster: kafka-cluster
spec:
  partitions: 3
  replicas: 3
```

Apply:

```bash
kubectl apply -f infrastructure/kafka/kafka-topic.yaml
```

Kiểm tra:

```bash
kubectl get kafkatopic -n kafka
kubectl get kafkatopic orders -n kafka -o yaml
kubectl describe kafkatopic orders -n kafka
```

Topic có ba partition và replication factor bằng ba:

```text
orders
  +-- partition 0: replicas trên ba broker
  +-- partition 1: replicas trên ba broker
  +-- partition 2: replicas trên ba broker
```

Topic cần Kafka cluster `Ready` và Topic Operator hoạt động. Nếu topic không được tạo, kiểm tra Kafka CR, Entity Operator, label liên kết cluster và Operator log trước khi debug storage.

## 12. Persistence và replication không giống nhau

### Replication

Replication tạo nhiều replica của partition trên các Kafka node:

```text
orders-0
  +-- Broker 0 replica
  +-- Broker 1 replica
  +-- Broker 2 replica
```

Nếu một broker mất, replica trên broker khác có thể tiếp tục phục vụ dữ liệu tùy trạng thái ISR và cluster.

### Persistence

Persistence giữ data của một node qua lifecycle của Pod:

```text
Kafka Pod 0 bị restart
        |
        v
PVC 0 vẫn tồn tại
        |
        v
Pod 0 mới mount PVC 0
```

### KRaft consensus

KRaft controller quorum giúp các controller thống nhất metadata cluster:

```text
Replication  = nhiều replica của partition data
Persistence  = data tồn tại qua Pod lifecycle
KRaft        = consensus cho cluster metadata
```

Ba cơ chế bổ trợ nhau nhưng không thay thế nhau.

Ví dụ replication trong cùng một Minikube node không bảo vệ dữ liệu khi toàn bộ Minikube node bị mất. Ngược lại, một PVC bền vững không tự tạo ra controller majority nếu quorum không còn đủ node.

## 13. Thực hành kiểm tra sau Pod restart

Chỉ thực hiện sau khi:

- Kafka CR đã `Ready`.
- Kafka Pod đang ổn định.
- PVC ở trạng thái `Bound`.
- Chưa có dữ liệu quan trọng cần giữ trong lab.

Lưu baseline:

```bash
kubectl get pods -n kafka -o wide
kubectl get pvc -n kafka -o wide
kubectl get pv
```

Chọn một Kafka Pod, không chọn Operator Pod:

```bash
kubectl get pods -n kafka
```

Xóa Pod:

```bash
kubectl delete pod <kafka-pod-name> -n kafka
```

Theo dõi Pod mới:

```bash
kubectl get pods -n kafka -w
```

Trong terminal khác, kiểm tra PVC:

```bash
kubectl get pvc -n kafka -w
```

Kết quả mong đợi:

```text
Pod cũ: Terminating / deleted
PVC:    vẫn Bound
Pod mới: Running và Ready
```

Sau khi Pod mới sẵn sàng, kiểm tra lại claim và volume:

```bash
kubectl get pvc -n kafka -o wide
kubectl get pods -n kafka -o wide
kubectl describe pod <new-kafka-pod-name> -n kafka
```

Xóa Pod không đồng nghĩa xóa PVC. Tuy nhiên, để chứng minh record thực sự còn lại, cần produce/consume dữ liệu; phần đó sẽ được thực hành đầy đủ ở phase producer/consumer.

## 14. `deleteClaim` và cảnh báo xóa PVC

Một số cấu hình Strimzi có thể dùng tùy chọn:

```yaml
deleteClaim: false
```

Tùy chọn này liên quan đến việc Strimzi xử lý claim trong một số lifecycle operation. Hãy kiểm tra schema và documentation của đúng Strimzi version trước khi thêm trường này.

Phân biệt:

```text
kubectl delete pod <pod> -n kafka
    -> xóa compute, PVC thường vẫn còn

kubectl delete pvc <pvc> -n kafka
    -> chủ động xóa claim, có thể làm mất đường dẫn tới data
```

Không xóa PVC để “sửa lỗi Pod” nếu chưa hiểu reclaim policy, PV backend và dữ liệu đang nằm trên volume. Trong lab, xóa PVC có thể là thao tác cleanup, nhưng không phải thao tác restart thông thường.

## 15. Troubleshooting

### Không có PVC

Kiểm tra theo thứ tự:

```bash
kubectl get kafka -n kafka
kubectl describe kafka kafka-cluster -n kafka
kubectl get kafkanodepool -n kafka -o yaml
kubectl get pods -n kafka
```

Nguyên nhân thường gặp:

- Kafka CR đang `NotReady`.
- Kafka version không được Strimzi hỗ trợ.
- NodePool vẫn dùng `ephemeral`.
- NodePool không liên kết với Kafka CR.
- Strimzi Operator chưa chạy hoặc đang lỗi.
- Manifest chưa được apply.

### PVC ở trạng thái `Pending`

```bash
kubectl describe pvc <pvc-name> -n kafka
kubectl get storageclass
kubectl get pv
kubectl get events -n kafka --sort-by=.lastTimestamp
```

Nguyên nhân có thể là:

- StorageClass không tồn tại.
- Provisioner không hoạt động.
- Dung lượng hoặc access mode không phù hợp.
- Minikube thiếu tài nguyên.
- PV tĩnh không có claim phù hợp.

Kiểm tra provisioner và default class:

```bash
kubectl get storageclass -o yaml
```

### PVC `Bound` nhưng Pod `Pending`

```bash
kubectl describe pod <kafka-pod-name> -n kafka
kubectl get events -n kafka --sort-by=.lastTimestamp
```

Có thể lỗi nằm ở mount, node scheduling, access mode hoặc volume attachment, không phải bước binding PVC. Đọc Events của Pod để phân biệt.

### Pod báo lỗi mount volume

```bash
kubectl describe pod <kafka-pod-name> -n kafka
kubectl describe pvc <pvc-name> -n kafka
kubectl describe pv <pv-name>
```

Kiểm tra volume đang thuộc node nào, access mode và provisioner. Với Minikube single-node, volume local thường không có khả năng attach như storage distributed production.

### Kafka Pod chạy nhưng topic không tạo được

Storage có thể đã đúng; kiểm tra thêm:

```bash
kubectl get kafka kafka-cluster -n kafka
kubectl get kafkatopic orders -n kafka
kubectl get deployment -n kafka
kubectl logs deployment/strimzi-cluster-operator -n kafka --tail=100
```

Đảm bảo Entity Operator có Topic Operator và `KafkaTopic` có label:

```yaml
strimzi.io/cluster: kafka-cluster
```

### PVC bị xóa ngoài ý muốn

Kiểm tra audit/event history nếu có, reclaim policy của PV và lifecycle resource của Strimzi:

```bash
kubectl get pv
kubectl describe pv <pv-name>
kubectl get kafkanodepool kafka-pool -n kafka -o yaml
```

Không cố tạo PVC có cùng tên hoặc sửa PV thủ công trước khi hiểu owner/reference và trạng thái Operator.

## 16. Các lệnh kiểm tra cần nhớ

### StorageClass

```bash
kubectl get storageclass
```

### PV

```bash
kubectl get pv
```

### PVC

```bash
kubectl get pvc -n kafka
kubectl get pvc -n kafka -o wide
kubectl describe pvc <pvc-name> -n kafka
```

### Kafka storage configuration

```bash
kubectl get kafkanodepool kafka-pool -n kafka -o yaml
```

### Kafka and Topic

```bash
kubectl get kafka -n kafka
kubectl get kafkatopic -n kafka
```

### Pod and Events

```bash
kubectl get pods -n kafka -o wide
kubectl get events -n kafka --sort-by=.lastTimestamp
```

## 17. Bài tập củng cố

### Bài tập 1: Vẽ luồng storage

Vẽ lại luồng sau bằng lời của bạn:

```text
KafkaNodePool -> PVC -> StorageClass -> provisioner -> PV -> Pod mount
```

Giải thích resource nào là request và resource nào đại diện cho storage đã được cấp.

### Bài tập 2: So sánh ba khái niệm

Viết ví dụ riêng cho:

```text
Replication:
Persistence:
KRaft consensus:
```

Không dùng cùng một câu trả lời cho cả ba khái niệm.

### Bài tập 3: Quan sát PVC

1. Ghi lại output của `kubectl get pvc -n kafka -o wide`.
2. Chọn một PVC và chạy `kubectl describe pvc`.
3. Tìm PV mà claim đang bind.
4. Tìm StorageClass và provisioner.
5. Đối chiếu PVC với Pod đang mount volume đó.

### Bài tập 4: Mô phỏng Pod restart

1. Lưu danh sách Pod và PVC trước khi xóa.
2. Xóa một Kafka Pod.
3. Theo dõi Pod mới.
4. Xác nhận PVC vẫn `Bound`.
5. Giải thích vì sao cần Phase producer/consumer để kiểm tra record thực sự còn lại.

## 18. Checklist hoàn thành Phase 4

- [ ] Đã hiểu Kafka partition log và data directory.
- [ ] Đã phân biệt ephemeral và persistent storage.
- [ ] Đã hiểu PV, PVC và StorageClass.
- [ ] Đã kiểm tra StorageClass `standard` hoặc class thực tế của cluster.
- [ ] `KafkaNodePool` dùng `persistent-claim`.
- [ ] Mỗi Kafka node có storage claim riêng.
- [ ] PVC ở trạng thái `Bound`.
- [ ] PV được tạo và liên kết với PVC.
- [ ] Đã kiểm tra Pod mount volume.
- [ ] Topic `orders` tồn tại với 3 partition và replication factor 3.
- [ ] Đã phân biệt replication, persistence và KRaft consensus.
- [ ] Đã xóa/recreate một Kafka Pod trong lab.
- [ ] PVC vẫn tồn tại sau Pod restart.
- [ ] Hiểu rằng xóa PVC có rủi ro mất dữ liệu.
- [ ] Hiểu giới hạn storage local của Minikube.

## 19. Kết nối sang Phase 5

Sau Phase 4, infrastructure đã có persistent storage và topic để bắt đầu xử lý record:

```text
Topic orders
  +-- partition 0
  +-- partition 1
  +-- partition 2
       |
       v
Producer -> Kafka -> Consumer
```

Phase 5 sẽ tập trung vào topic, partition, replication, leader/follower, offset và thao tác produce/consume bằng Kafka CLI ngay trong cluster. Khi đó chúng ta có thể kiểm tra persistence bằng dữ liệu thật thay vì chỉ kiểm tra PVC và Pod lifecycle.