# Phase 1: Nền tảng Kafka trên Kubernetes

Phase này chuẩn bị môi trường Minikube và xây dựng mô hình tư duy cần thiết trước khi cài Strimzi hoặc triển khai Kafka. Mục tiêu không phải là chạy Kafka ngay, mà là hiểu các thành phần sẽ xuất hiện trong những phase sau và xác nhận cluster Kubernetes hoạt động bình thường.

## 1. Mục tiêu học tập

Sau Phase 1, bạn có thể:

- Giải thích vai trò của Kafka trong hệ thống event-driven.
- Phân biệt cluster, broker, topic, partition, replica, leader và offset.
- Mô tả sự khác nhau giữa ZooKeeper và KRaft.
- Giải thích Operator pattern và vai trò của Strimzi.
- Khởi động hoặc kiểm tra Minikube bằng Docker driver.
- Tạo namespace `kafka` bằng manifest Kubernetes.
- Kiểm tra resource trong namespace và xử lý các lỗi môi trường cơ bản.

## 2. Kiến trúc mục tiêu

Trong các phase tiếp theo, Strimzi Operator sẽ chạy trong Kubernetes và quản lý một Kafka cluster thông qua các Custom Resource.

```text
Kubernetes API
      |
      v
Strimzi Operator
      |
      v
Kafka custom resource
      |
      v
Kafka cluster
  |       |       |
  v       v       v
Broker 0 Broker 1 Broker 2
  |       |       |
  +-------+-------+
          |
       Storage
```

Luồng dữ liệu cơ bản:

```text
Producer -> Topic -> Partition -> Consumer
                         |
                      Offset
```

Trong Phase 1, chúng ta mới chuẩn bị namespace. Chưa có Operator, Kafka Pod, topic hay consumer nào được tạo.

## 3. Kiến thức Kafka cần nắm

### 3.1. Kafka là gì?

Kafka là một distributed event streaming platform. Producer ghi các event vào Kafka; consumer đọc các event đó theo nhu cầu của mình.

Ví dụ với hệ thống đặt hàng:

```text
Order Service -> OrderCreated -> Kafka
                                  |
                  +---------------+---------------+
                  v               v               v
             Payment Service Inventory Service Notification Service
```

Kafka giúp Order Service không phải gọi trực tiếp từng service downstream. Các consumer có thể được thêm hoặc thay đổi mà producer không cần biết chi tiết.

### 3.2. Cluster và broker

Kafka cluster là tập hợp nhiều Kafka broker. Mỗi broker là một process Kafka có định danh riêng và chịu trách nhiệm lưu trữ, nhận hoặc phục vụ record.

Trong Kubernetes, một broker thường chạy bên trong một Pod, nhưng hai khái niệm không đồng nhất:

- Pod là đơn vị triển khai và quản lý của Kubernetes.
- Broker là thành phần ứng dụng Kafka chạy trong Pod.
- Khi Pod bị thay thế, broker có thể khởi động lại với cùng định danh và storage tùy cấu hình.

### 3.3. Topic và record

Topic là tên logic của luồng dữ liệu, ví dụ `orders`, `payments` hoặc `notifications`. Producer gửi record vào topic; consumer đăng ký topic để đọc record.

Một record thường có value và có thể có key, timestamp, headers. Kafka không xử lý record như một hàng trong cơ sở dữ liệu; nó ghi record tuần tự vào log của partition.

### 3.4. Partition và thứ tự

Topic được chia thành một hoặc nhiều partition:

```text
orders
  +-- partition 0: offset 0, 1, 2, 3, ...
  +-- partition 1: offset 0, 1, 2, 3, ...
  +-- partition 2: offset 0, 1, 2, 3, ...
```

Các điểm quan trọng:

- Mỗi partition là một ordered log.
- Thứ tự được đảm bảo trong cùng một partition, không phải trên toàn bộ topic.
- Nhiều partition cho phép Kafka phân phối tải và tăng khả năng xử lý song song.
- Key thường được dùng để quyết định record đi vào partition nào; các record có cùng key thường đi cùng partition.

### 3.5. Offset

Offset là số thứ tự của record trong một partition. Consumer dùng offset để biết đã đọc đến đâu và có thể tiếp tục sau khi restart.

Ví dụ:

```text
partition 0
offset 0 -> Order A
offset 1 -> Order B
offset 2 -> Order C
```

Offset là theo từng partition. `offset 2` của partition 0 không có quan hệ thứ tự với `offset 2` của partition 1.

### 3.6. Replication, leader và replica

Replication factor cho biết mỗi partition có bao nhiêu bản sao. Với replication factor bằng 3, một partition có thể được lưu trên ba broker khác nhau.

```text
Partition 0
  +-- Broker 0: leader
  +-- Broker 1: replica
  +-- Broker 2: replica
```

Leader phục vụ request đọc và ghi của partition. Replica giữ bản sao dữ liệu và có thể được bầu làm leader mới khi broker hiện tại gặp sự cố. Replication giúp tăng khả năng chịu lỗi, nhưng không thay thế backup và không làm mất nhu cầu theo dõi storage.

### 3.7. KRaft

Kiến trúc Kafka cũ dùng ZooKeeper để lưu metadata và điều phối một số hoạt động của cluster:

```text
ZooKeeper <-> Kafka brokers
```

Kafka hiện đại dùng KRaft, trong đó Kafka tự quản lý metadata thông qua các controller node. Một node có thể đảm nhiệm cả hai role:

```text
Kafka node = broker + controller
```

Project này dùng KRaft vì đây là hướng triển khai Kafka hiện đại và không cần thêm ZooKeeper. Trong môi trường học tập Minikube, mô hình combined broker/controller giúp giảm số lượng thành phần phải vận hành.

## 4. Kubernetes và Strimzi

### 4.1. Vì sao không tự viết toàn bộ YAML?

Kafka có nhiều yêu cầu vận hành hơn một workload stateless thông thường: storage ổn định, network listener, rolling update, cấu hình broker, health check, replication và xử lý thay đổi cluster.

Nếu tự quản lý, bạn phải phối hợp nhiều resource như StatefulSet, Service, PVC, ConfigMap và Secret. Cách đó hữu ích để hiểu Kubernetes primitives, nhưng dễ bỏ sót logic đặc thù của Kafka.

### 4.2. Operator pattern

Kubernetes mặc định hiểu các resource như Pod, Deployment, Service và StatefulSet. Kubernetes không tự hiểu cách tạo hoặc cân bằng một Kafka cluster.

Operator mở rộng Kubernetes bằng:

- CRD (Custom Resource Definition): định nghĩa loại resource mới.
- Controller: theo dõi desired state và actual state.
- Domain knowledge: logic riêng để vận hành ứng dụng.

Strimzi là Kafka Operator. Khi bạn khai báo một resource `kind: Kafka`, Strimzi đọc resource đó rồi tạo và duy trì các resource Kubernetes cần thiết.

```text
Kafka CR -> Strimzi controller -> StatefulSet/Pod/Service/Storage
```

### 4.3. Các custom resource sẽ gặp

| Resource | Vai trò |
| --- | --- |
| `Kafka` | Mô tả Kafka cluster và cấu hình broker/controller |
| `KafkaTopic` | Mô tả topic, partition và replication factor |
| `KafkaUser` | Mô tả user và cơ chế xác thực/quyền truy cập |

Phase 1 chưa tạo các resource này. Chúng sẽ được dùng từ Phase 2 và Phase 3.

## 5. Chuẩn bị môi trường

### 5.1. Công cụ cần có

Kiểm tra các công cụ:

```bash
minikube version
kubectl version --client
docker version
```

Nếu một lệnh không tồn tại, hãy cài công cụ tương ứng trước khi tiếp tục. Trên macOS, Docker Desktop cần đang chạy nếu sử dụng Docker driver.

### 5.2. Kiểm tra Minikube

```bash
minikube status
```

Cluster đang hoạt động thường có các trạng thái tương tự:

```text
host: Running
kubelet: Running
apiserver: Running
kubeconfig: Configured
```

Nếu cluster chưa chạy, khởi động bằng:

```bash
minikube start --driver=docker
```

Không cần chạy lại lệnh `minikube start` nếu cluster đã ở trạng thái `Running`. Xác nhận node:

```bash
kubectl get nodes
```

Kết quả mong đợi là node `minikube` ở trạng thái `Ready`.

### 5.3. Kiểm tra context

Trước khi apply manifest, xác nhận `kubectl` đang trỏ đến đúng cluster:

```bash
kubectl config current-context
kubectl cluster-info
```

Với Minikube, context thường là `minikube`. Bước này tránh apply nhầm resource vào một cluster khác.

## 6. Chuẩn bị cấu trúc project

Từ thư mục gốc repository:

```bash
mkdir -p docs
mkdir -p infrastructure/namespace
mkdir -p infrastructure/strimzi
mkdir -p infrastructure/kafka
mkdir -p infrastructure/storage
mkdir -p clients/producer
mkdir -p clients/consumer
mkdir -p apps/order-service
mkdir -p scripts
```

Cấu trúc dự kiến:

```text
.
├── apps/
│   └── order-service/
├── clients/
│   ├── consumer/
│   └── producer/
├── docs/
├── infrastructure/
│   ├── kafka/
│   ├── namespace/
│   ├── storage/
│   └── strimzi/
└── scripts/
```

## 7. Tạo namespace Kafka

Tạo file `infrastructure/namespace/namespace.yaml` với nội dung:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: kafka
```

Apply manifest:

```bash
kubectl apply -f infrastructure/namespace/namespace.yaml
```

Kiểm tra namespace:

```bash
kubectl get namespace kafka
kubectl get ns kafka
```

Lệnh thứ hai chỉ là dạng viết tắt của lệnh thứ nhất. Kết quả cần có namespace `kafka` với status `Active`.

Namespace giúp gom các resource liên quan và giới hạn phạm vi thao tác:

```bash
kubectl get pods -n kafka
kubectl get all -n kafka
```

Ở cuối Phase 1, `kubectl get all -n kafka` có thể trả về `No resources found`. Đây là kết quả đúng vì Kafka và Strimzi chưa được cài.

## 8. Bài kiểm tra cuối phase

Chạy lần lượt:

```bash
minikube status
kubectl get nodes
kubectl config current-context
kubectl get ns kafka
kubectl get all -n kafka
```

Checklist kết quả:

- Minikube có `host`, `kubelet` và `apiserver` ở trạng thái `Running`.
- Node `minikube` ở trạng thái `Ready`.
- Context hiện tại là `minikube` hoặc context cluster bạn chủ động chọn.
- Namespace `kafka` ở trạng thái `Active`.
- Chưa có Kafka Pod nào là bình thường ở phase này.

Có thể kiểm tra manifest trước khi apply bằng:

```bash
kubectl apply --dry-run=client -f infrastructure/namespace/namespace.yaml
```

## 9. Bài tập củng cố

### Bài tập 1: Giải thích kiến trúc

Trả lời bằng lời của bạn:

1. Vì sao Kafka broker và Kubernetes Pod không phải là cùng một khái niệm?
2. Vì sao thứ tự record chỉ được đảm bảo trong một partition?
3. Replication factor bằng 3 giúp ích gì khi một broker bị lỗi?
4. Strimzi bổ sung điều gì mà Kubernetes mặc định không có?

### Bài tập 2: Quan sát namespace

Chạy:

```bash
kubectl describe namespace kafka
kubectl get resourcequota -n kafka
kubectl get limitrange -n kafka
```

Ghi nhận rằng namespace mới chưa có ResourceQuota hoặc LimitRange. Đây là cơ hội để phân biệt namespace với cơ chế giới hạn tài nguyên.

### Bài tập 3: Kiểm tra tính lặp lại

Apply manifest namespace lần thứ hai:

```bash
kubectl apply -f infrastructure/namespace/namespace.yaml
```

Lệnh vẫn an toàn vì `kubectl apply` hướng tới desired state. Namespace đã tồn tại sẽ không tạo thêm một namespace thứ hai.

## 10. Troubleshooting

### `minikube: command not found`

Minikube chưa được cài hoặc chưa nằm trong `PATH`. Cài Minikube rồi mở lại Terminal nếu cần.

### Docker driver không khởi động

Kiểm tra Docker Desktop đang chạy:

```bash
docker info
```

Nếu Docker daemon chưa hoạt động, Minikube không thể tạo node bằng Docker driver.

### `kubectl get nodes` không kết nối được cluster

Kiểm tra trạng thái và context:

```bash
minikube status
kubectl config current-context
kubectl cluster-info
```

Nếu cluster đã dừng, chạy lại:

```bash
minikube start --driver=docker
```

### Namespace không được tạo

Kiểm tra file và YAML:

```bash
ls -l infrastructure/namespace/namespace.yaml
kubectl apply --dry-run=client -f infrastructure/namespace/namespace.yaml
```

Đảm bảo file có `apiVersion`, `kind`, `metadata.name` và không bị để trống.

### `No resources found` khi chạy `kubectl get all -n kafka`

Đây không phải lỗi trong Phase 1. Namespace chỉ là container logic; Strimzi và Kafka sẽ được cài ở các phase sau.

### Apply nhầm cluster

Luôn xem context trước khi apply:

```bash
kubectl config get-contexts
kubectl config current-context
```

Chuyển về Minikube nếu cần:

```bash
kubectl config use-context minikube
```

## 11. Dọn dẹp

Nếu muốn xóa namespace và toàn bộ resource bên trong namespace:

```bash
kubectl delete namespace kafka
```

Lệnh này phù hợp khi namespace chỉ chứa resource của project học tập. Không chạy trên cluster dùng chung nếu chưa kiểm tra resource bên trong.

Dừng Minikube nhưng giữ dữ liệu cluster:

```bash
minikube stop
```

Xóa toàn bộ cluster Minikube:

```bash
minikube delete
```

## 12. Checklist hoàn thành Phase 1

- [ ] Đã cài và kiểm tra `minikube`, `kubectl`, Docker.
- [ ] Đã hiểu broker, topic, partition, replication và offset.
- [ ] Đã hiểu KRaft và lý do project không dùng ZooKeeper.
- [ ] Đã hiểu Operator pattern và vai trò của Strimzi.
- [ ] Minikube đang chạy với node ở trạng thái `Ready`.
- [ ] Context `kubectl` trỏ đến cluster mong muốn.
- [ ] Namespace `kafka` tồn tại và ở trạng thái `Active`.
- [ ] Đã xác nhận namespace chưa có workload là bình thường.
- [ ] Đã chạy bài tập hoặc tự giải thích lại kiến trúc bằng lời của mình.

## 13. Kết nối sang Phase 2

Phase tiếp theo sẽ cài Strimzi Operator. Khi đó, các khái niệm ở Phase 1 sẽ map vào resource thật:

```text
CRD -> Kafka/KafkaTopic/KafkaUser
Operator -> controller reconcile desired state
Kafka CR -> Kafka cluster
```

Trước khi sang Phase 2, hãy chắc chắn rằng namespace `kafka` đã tồn tại và bạn biết cách kiểm tra context Kubernetes. Hai điều này là nền tảng để tránh cài Operator vào sai cluster hoặc sai namespace.