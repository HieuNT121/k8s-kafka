# Phase 3: Deploy Kafka Cluster với KRaft

Phase này chuyển project từ trạng thái chỉ có Strimzi Operator sang một Kafka cluster thực sự chạy trên Kubernetes. Chúng ta dùng Kafka hiện đại với KRaft, không dùng ZooKeeper, và mô hình `KafkaNodePool` gồm ba node có đồng thời hai role `broker` và `controller`.

> Đây là cluster lab trên Minikube. Storage hiện tại là `ephemeral`, vì vậy không dùng cấu hình này cho production và không xem ba Kafka node trên một Minikube node là high availability thực sự.

## 1. Kết quả mong đợi

Trước Phase 3:

```text
Kubernetes
    |
    +-- namespace kafka
            |
            +-- Strimzi Cluster Operator
            +-- Strimzi CRDs
```

Sau Phase 3:

```text
Kubernetes
    |
    +-- namespace kafka
            |
            +-- Strimzi Cluster Operator
            +-- Kafka cluster: kafka-cluster
                    |
                    +-- node 0: broker + controller
                    +-- node 1: broker + controller
                    +-- node 2: broker + controller
```

Kết quả không chỉ là ba Pod `Running`. Cần kiểm tra đồng thời:

- Kafka CR tồn tại và có trạng thái phù hợp.
- KafkaNodePool tồn tại với ba replica và đúng roles.
- Kafka node Pod sẵn sàng.
- Service, Secret và ConfigMap cần thiết đã được tạo.
- KRaft controller quorum có đủ majority.

## 2. Mục tiêu học tập

Sau phase này, bạn có thể:

- Giải thích KRaft thay thế ZooKeeper như thế nào.
- Phân biệt Kafka broker, controller và Kafka node.
- Hiểu controller quorum, leader election và majority.
- Phân biệt data log với KRaft metadata log.
- Hiểu vai trò của `Kafka` và `KafkaNodePool` trong Strimzi.
- Kiểm tra version Strimzi và API version trước khi apply manifest.
- Deploy cluster bằng hai Custom Resource.
- Quan sát Strimzi reconciliation từ Kafka CR tới Pod và Service.
- Mô phỏng việc một Pod bị xóa và giải thích cơ chế self-healing.
- Nhận biết giới hạn của cluster ba node chạy trên một Minikube node.

## 3. KRaft thay thế ZooKeeper

### 3.1. Kiến trúc cũ

Ở các phiên bản Kafka cũ, Kafka broker phụ thuộc ZooKeeper cho các nhiệm vụ coordination và metadata:

```text
                 ZooKeeper
                /    |    \
               v     v     v
           Kafka 0 Kafka 1 Kafka 2
```

Khi đó hệ thống có hai distributed system cần vận hành:

```text
Kafka cluster + ZooKeeper ensemble
```

### 3.2. Kiến trúc KRaft

KRaft, viết tắt của Kafka Raft metadata mode, đưa việc quản lý cluster metadata vào chính Kafka thông qua controller quorum:

```text
Kafka cluster
     |
     +-- Controller 0
     +-- Controller 1
     +-- Controller 2
```

Không còn thành phần ZooKeeper:

```text
Kafka node = broker role + controller role
```

KRaft không phải là một loại broker mới. Đây là cơ chế Kafka dùng để quản lý metadata và coordination bằng Raft.

## 4. Broker, controller và node

### 4.1. Broker

Broker xử lý data plane của Kafka:

- Nhận request từ producer.
- Phục vụ request từ consumer.
- Lưu partition log.
- Quản lý leader và replica data theo metadata cluster.

```text
Producer -> Broker -> Topic partition -> Consumer
```

### 4.2. Controller

Controller quản lý control plane và cluster metadata, chẳng hạn:

- Broker nào đang tồn tại.
- Topic và partition nào tồn tại.
- Replica assignment của partition.
- Broker nào đang là leader.
- Các thay đổi cấu hình cluster.

### 4.3. Kafka node

Kafka node là một tiến trình/instance Kafka có một hoặc nhiều role. Strimzi hỗ trợ các kiểu node như:

```text
controller-only
broker-only
broker + controller
```

Trong Phase 3, mỗi node là dual-role:

```text
Node 0 = broker + controller
Node 1 = broker + controller
Node 2 = broker + controller
```

Mô hình dual-role phù hợp cho lab nhỏ. Production thường cân nhắc tách controller pool và broker pool để sizing, failure domain và vận hành rõ ràng hơn.

## 5. KRaft controller quorum

Các controller tạo thành một quorum để duy trì metadata nhất quán:

```text
Controller 0
Controller 1
Controller 2
       |
       v
KRaft controller quorum
```

Trong quorum có một leader. Leader điều phối việc ghi metadata; các controller còn lại replicate metadata và có thể tham gia leader election khi leader lỗi.

### 5.1. Majority

Quorum cần majority để tiếp tục hoạt động. Công thức:

```text
majority = floor(number_of_controllers / 2) + 1
```

Với ba controller:

```text
3 controllers -> majority = 2
```

Vì vậy:

```text
Mất 1 controller -> còn 2/3 -> vẫn có majority
Mất 2 controller -> còn 1/3 -> mất majority
```

Số controller lẻ thường được chọn để đạt fault tolerance hợp lý mà không tăng số node quá mức.

### 5.2. Metadata log và data log

Kafka có hai loại log cần phân biệt:

```text
Kafka
|
+-- Data log
|     +-- user records trong topic partitions
|
+-- KRaft metadata log
      +-- topic metadata
      +-- partition metadata
      +-- replica assignment
      +-- leader information
      +-- cluster configuration
```

KRaft metadata log không chứa toàn bộ message của ứng dụng. Message vẫn nằm trong Kafka partition logs.

Ví dụ data log:

```text
orders / partition 0
  offset 0 -> Order A
  offset 1 -> Order B
  offset 2 -> Order C
```

Metadata có thể mô tả:

```text
orders / partition 0
  replicas = [0, 1, 2]
  leader = 0
```

## 6. `Kafka` và `KafkaNodePool` trong Strimzi

Strimzi tách cấu hình cluster-level và node-level thành hai Custom Resource.

### `Kafka`

Kafka CR mô tả những phần chung của cluster:

- Kafka version.
- Metadata version.
- Listener.
- Cluster-level configuration.
- Entity Operator.

### `KafkaNodePool`

KafkaNodePool mô tả nhóm node có cấu hình chung:

- Số replica.
- Roles.
- Storage.
- CPU và memory.
- Các tùy chọn node-level.

```text
Kafka CR
  +-- version
  +-- listeners
  +-- metadata/configuration
  +-- entityOperator

KafkaNodePool
  +-- replicas
  +-- roles
  +-- storage
  +-- resources
```

Tách hai resource giúp sau này tạo các pool khác nhau, ví dụ controller-only pool và broker-only pool với sizing riêng.

## 7. Kiểm tra version trước khi deploy

API và schema Strimzi thay đổi theo release. Không nên lấy mù `apiVersion`, Kafka version hoặc metadata version từ một tutorial khác.

### 7.1. Kiểm tra image Operator

```bash
kubectl get deployment strimzi-cluster-operator \
  -n kafka \
  -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
```

### 7.2. Kiểm tra API version của Kafka CRD

```bash
kubectl get crd kafkas.kafka.strimzi.io \
  -o jsonpath='{.spec.versions[*].name}{"\n"}'
```

### 7.3. Kiểm tra API version của KafkaNodePool CRD

```bash
kubectl get crd kafkanodepools.kafka.strimzi.io \
  -o jsonpath='{.spec.versions[*].name}{"\n"}'
```

Trong repo hiện tại, các manifest dùng:

```yaml
apiVersion: kafka.strimzi.io/v1
```

Nếu cluster chỉ expose version khác, phải điều chỉnh manifest theo release đang cài. Không apply YAML khi CRD không hỗ trợ `kafka.strimzi.io/v1`.

### 7.4. Kiểm tra Kafka version được hỗ trợ

Kafka version trong manifest phải được Strimzi release đang chạy hỗ trợ. Repo hiện tại dùng:

```yaml
version: 4.0.0
metadataVersion: "4.0-IV0"
```

Đây là cấu hình cần xác minh với tài liệu đúng release của Operator trước khi apply. Nếu Operator không hỗ trợ `4.0.0`, chọn version Kafka được release đó hỗ trợ và metadata version tương ứng.

## 8. Manifest `KafkaNodePool`

File hiện tại:

```text
infrastructure/kafka/kafka-node-pool.yaml
```

Nội dung:

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: kafka-pool
  namespace: kafka
  labels:
    strimzi.io/cluster: kafka-cluster
spec:
  replicas: 3
  roles:
    - controller
    - broker
  storage:
    type: ephemeral
  resources:
    requests:
      cpu: "500m"
      memory: 768Mi
    limits:
      cpu: "1"
      memory: 1Gi
```

### Giải thích các trường

`kind: KafkaNodePool` là CRD của Strimzi, không phải StatefulSet mà người dùng tự tạo.

`metadata.name: kafka-pool` là tên pool. Label sau đây liên kết pool với Kafka cluster:

```yaml
strimzi.io/cluster: kafka-cluster
```

Tên label phải trùng với `metadata.name` của Kafka CR. Nếu label trỏ tới cluster khác hoặc viết sai, Operator có thể không liên kết pool như mong muốn.

`replicas: 3` mô tả ba Kafka node, không chỉ đơn giản là ba replica của một Deployment stateless.

`roles` khai báo mỗi node làm cả broker và controller:

```yaml
roles:
  - controller
  - broker
```

`storage.type: ephemeral` phù hợp cho lab vì đơn giản và nhanh. Dữ liệu có thể mất khi Pod/node được thay thế. Phase 4 sẽ chuyển sang PersistentVolumeClaim và kiểm tra persistence.

Requests và limits hiện tại là sizing cho Minikube lab, không phải sizing production. Tổng request lý thuyết của ba node là khoảng `1.5 CPU` và `2304Mi` memory, chưa tính Operator và các process khác.

## 9. Manifest `Kafka`

File hiện tại:

```text
infrastructure/kafka/kafka-cluster.yaml
```

Nội dung:

```yaml
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: kafka-cluster
  namespace: kafka
  annotations:
    strimzi.io/node-pools: enabled
    strimzi.io/kraft: enabled
spec:
  kafka:
    version: 4.0.0
    metadataVersion: "4.0-IV0"
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
  entityOperator:
    topicOperator: {}
    userOperator: {}
```

### Hai annotation quan trọng

```yaml
strimzi.io/node-pools: enabled
strimzi.io/kraft: enabled
```

Annotation đầu tiên cho biết cluster dùng KafkaNodePool. Annotation thứ hai bật KRaft thay cho ZooKeeper. Cả hai phải phù hợp với version Strimzi đang chạy.

### Listener

```yaml
listeners:
  - name: plain
    port: 9092
    type: internal
    tls: false
```

Listener này tạo endpoint Kafka nội bộ Kubernetes. Nó phù hợp để các client chạy trong cluster kết nối. Đây chưa phải cấu hình expose Kafka ra máy host hoặc Internet; external listener sẽ được học ở phase sau.

### Entity Operator

```yaml
entityOperator:
  topicOperator: {}
  userOperator: {}
```

Topic Operator cho phép quản lý topic thông qua `KafkaTopic`. User Operator cho phép quản lý user thông qua `KafkaUser`. Hai thành phần này chuẩn bị cho các phase topic, producer/consumer và security.

## 10. Validate và apply theo thứ tự

### 10.1. Kiểm tra Kafka resource hiện tại

```bash
kubectl get kafka -n kafka
```

Trước khi deploy, thường chưa có resource nào.

### 10.2. Validate manifest phía client

```bash
kubectl apply --dry-run=client \
  -f infrastructure/kafka/kafka-node-pool.yaml

kubectl apply --dry-run=client \
  -f infrastructure/kafka/kafka-cluster.yaml
```

`dry-run=client` kiểm tra cú pháp và cấu trúc cơ bản. Nó không chứng minh rằng server có CRD/version/Kafka version phù hợp.

### 10.3. Apply NodePool trước

```bash
kubectl apply \
  -f infrastructure/kafka/kafka-node-pool.yaml
```

Kiểm tra:

```bash
kubectl get kafkanodepool -n kafka
kubectl describe kafkanodepool kafka-pool -n kafka
```

### 10.4. Apply Kafka CR

```bash
kubectl apply \
  -f infrastructure/kafka/kafka-cluster.yaml
```

Kiểm tra ngay:

```bash
kubectl get kafka -n kafka
kubectl describe kafka kafka-cluster -n kafka
```

Trong lúc reconciliation, status có thể chưa `Ready`. Đây là quá trình bất đồng bộ; cần quan sát tiếp thay vì kết luận ngay sau lệnh apply.

## 11. Quan sát Strimzi reconciliation

Strimzi biến các desired state trong hai CR thành resource Kubernetes thực tế:

```text
KafkaNodePool + Kafka CR
          |
          v
Kubernetes API Server
          |
          v
Strimzi Operator watch/reconcile
          |
          +-- Kafka nodes
          +-- Services
          +-- ConfigMaps
          +-- Secrets
          +-- Entity Operator resources
```

Theo dõi Pod:

```bash
kubectl get pods -n kafka -w
```

Các trạng thái tạm thời có thể thấy:

```text
Pending -> ContainerCreating -> Running
```

Tên Pod phụ thuộc version Strimzi và node pool. Không hard-code tên Pod nếu viết script.

Theo dõi Operator log trong terminal khác:

```bash
kubectl logs deployment/strimzi-cluster-operator \
  -n kafka \
  --tail=100 \
  -f
```

Dừng lệnh follow bằng `Ctrl+C`.

## 12. Kiểm tra resources sau khi deploy

### 12.1. Kafka và NodePool

```bash
kubectl get kafka -n kafka
kubectl get kafkanodepool -n kafka
kubectl describe kafka kafka-cluster -n kafka
kubectl describe kafkanodepool kafka-pool -n kafka
```

Chú ý `Status` và `Conditions`, không chỉ nhìn `NAME`.

### 12.2. Pod

```bash
kubectl get pods -n kafka -o wide
```

Mục tiêu lab là Operator và ba Kafka node có `READY 1/1`, `STATUS Running`. Tên Pod chính xác có thể khác release.

### 12.3. Service, ConfigMap và Secret

```bash
kubectl get svc -n kafka
kubectl get configmap -n kafka
kubectl get secret -n kafka
```

Không nên xóa hoặc sửa resource do Strimzi quản lý trực tiếp nếu chưa hiểu owner/reference. Thay đổi nên thực hiện qua Kafka hoặc KafkaNodePool CR.

### 12.4. Toàn bộ resource namespaced

```bash
kubectl get all -n kafka
```

Lệnh này không hiển thị mọi loại resource, vì vậy vẫn cần kiểm tra riêng CR, ConfigMap, Secret và PVC khi Phase 4 bắt đầu.

### 12.5. Events

```bash
kubectl get events \
  -n kafka \
  --sort-by=.lastTimestamp
```

Events hữu ích để nhận biết lỗi scheduling, image pull, probe, PVC hoặc permission.

## 13. Kiểm tra readiness nhiều tầng

Không kết luận Kafka hoạt động chỉ vì Pod `Running`. Kiểm tra theo tầng:

```text
Tầng 1: Kubernetes node Ready
Tầng 2: Strimzi Operator Running
Tầng 3: KafkaNodePool tồn tại và đúng roles
Tầng 4: Kafka Pod Ready
Tầng 5: Kafka CR có condition phù hợp
Tầng 6: KRaft controller quorum có majority
```

Các command nền tảng:

```bash
kubectl get nodes
kubectl get pods -n kafka
kubectl get kafkanodepool -n kafka
kubectl get kafka kafka-cluster -n kafka -o yaml
kubectl get svc -n kafka
```

`kubectl get kafka ... -o yaml` cho phép đọc phần `status.conditions` và các thông tin Operator ghi lại. Schema status có thể khác nhau theo Strimzi version, nên đọc field thực tế thay vì giả định một output cố định.

## 14. Mô phỏng failure một Pod

Sau khi cluster đã ổn định, lấy danh sách Pod:

```bash
kubectl get pods -n kafka -o wide
```

Chọn một Kafka Pod, sau đó xóa Pod:

```bash
kubectl delete pod <kafka-pod-name> -n kafka
```

Theo dõi quá trình tạo lại:

```bash
kubectl get pods -n kafka -w
```

Điều đang xảy ra:

```text
Pod bị xóa
    |
    v
Kubernetes/Strimzi phát hiện actual state lệch desired state
    |
    v
Kafka node được tạo lại
```

Đây là self-healing ở tầng Kubernetes/Strimzi. Nó khác với KRaft quorum:

```text
Kubernetes/Strimzi: quản lý Pod, node process và resource
KRaft: quản lý Kafka metadata và controller consensus
```

Với storage `ephemeral`, việc thay thế Pod có thể làm mất data của node. Vì vậy chỉ chạy thí nghiệm này khi chưa có dữ liệu quan trọng.

Không mô phỏng mất hai controller trong phase này. Mục tiêu là quan sát một Pod được recreate, không phải cố tình làm mất majority trên Minikube.

## 15. Giới hạn của Minikube lab

Minikube thường chỉ có một Kubernetes node:

```text
Kubernetes node: minikube
    +-- Kafka node 0
    +-- Kafka node 1
    +-- Kafka node 2
```

Vì vậy ba Kafka node không đồng nghĩa ba máy vật lý hoặc ba failure domain. Nếu `minikube` node bị mất, cả ba Kafka node có thể bị ảnh hưởng.

Cluster này phù hợp để học:

- Kafka và KRaft.
- Strimzi reconciliation.
- Kubernetes Custom Resource.
- Listener và service nội bộ.
- Pod lifecycle.

Không nên dùng nó để kết luận về:

- High availability production.
- Fault tolerance giữa các availability zone.
- Hiệu năng Kafka production.
- Durability khi dùng ephemeral storage.

Production thường cần nhiều Kubernetes node, topology spread/rack awareness, persistent storage, resource sizing, monitoring và security phù hợp.

## 16. Troubleshooting

### `the server doesn't have a resource type "kafkanodepool"`

CRD KafkaNodePool chưa tồn tại hoặc API version không phù hợp:

```bash
kubectl get crd kafkanodepools.kafka.strimzi.io
kubectl get crd kafkanodepools.kafka.strimzi.io \
  -o jsonpath='{.spec.versions[*].name}{"\n"}'
```

Kiểm tra lại Strimzi release và CRD trước khi chỉnh manifest.

### Kafka CR bị reject vì API version

```bash
kubectl get crd kafkas.kafka.strimzi.io \
  -o jsonpath='{.spec.versions[*].name}{"\n"}'
```

Đảm bảo `apiVersion` trong YAML có version đang được CRD expose.

### Kafka version không được hỗ trợ

Operator có thể tạo Kafka CR nhưng báo lỗi trong `status` hoặc log vì Kafka version không nằm trong danh sách hỗ trợ. Kiểm tra documentation đúng release và đổi `spec.kafka.version` cùng metadata version tương thích.

### Pod ở `Pending`

```bash
kubectl describe pod <pod-name> -n kafka
kubectl describe node minikube
```

Đọc `Events`, đặc biệt là `FailedScheduling`. Kiểm tra CPU và memory Minikube có đủ cho Operator, entity operator và ba Kafka node hay không.

### `ImagePullBackOff` hoặc `ErrImagePull`

```bash
kubectl describe pod <pod-name> -n kafka
docker info
```

Events sẽ cho biết lỗi image, registry, DNS, proxy hoặc network. Không tự đổi image tag nếu chưa xác định compatibility với Strimzi release.

### Pod `Running` nhưng chưa `Ready`

```bash
kubectl describe pod <pod-name> -n kafka
kubectl logs <pod-name> -n kafka --tail=100
kubectl get events -n kafka --sort-by=.lastTimestamp
```

Có thể Kafka vẫn đang khởi động, thiếu tài nguyên, listener chưa sẵn sàng hoặc cấu hình version không hợp lệ.

### Kafka CR không `Ready`

```bash
kubectl describe kafka kafka-cluster -n kafka
kubectl get kafka kafka-cluster -n kafka -o yaml
kubectl logs deployment/strimzi-cluster-operator -n kafka --tail=100
```

Đọc `status.conditions` và đối chiếu với Operator log. Không chỉ dựa vào số lượng Pod.

### Không thấy đủ ba Kafka node

```bash
kubectl get kafkanodepool kafka-pool -n kafka -o yaml
kubectl get pods -n kafka -o wide
kubectl get events -n kafka --sort-by=.lastTimestamp
```

Kiểm tra `replicas`, roles, tài nguyên node và reconciliation error.

### KafkaNodePool không liên kết với Kafka cluster

Kiểm tra label:

```bash
kubectl get kafkanodepool kafka-pool -n kafka \
  -o jsonpath='{.metadata.labels.strimzi\.io/cluster}{"\n"}'
```

Giá trị phải là `kafka-cluster`, trùng với Kafka CR `metadata.name`.

### Xóa Pod nhưng Pod không được tạo lại

Kiểm tra owner và desired state:

```bash
kubectl get pods -n kafka
kubectl get kafkanodepool kafka-pool -n kafka -o yaml
kubectl get kafka kafka-cluster -n kafka -o yaml
kubectl logs deployment/strimzi-cluster-operator -n kafka --tail=100
```

Nếu node pool hoặc Kafka CR đang bị lỗi, Strimzi có thể chưa thể hoàn tất reconciliation.

## 17. Bài tập củng cố

### Bài tập 1: Broker và controller

Giải thích bằng lời của bạn:

1. Broker xử lý loại request nào?
2. Controller quản lý loại metadata nào?
3. Vì sao một Kafka node có thể giữ cả hai role?

### Bài tập 2: Quorum

Với ba controller, trả lời:

1. Majority là bao nhiêu?
2. Mất một controller thì điều gì xảy ra?
3. Mất hai controller thì điều gì xảy ra?

### Bài tập 3: Đọc manifest

Mở hai file:

```text
infrastructure/kafka/kafka-node-pool.yaml
infrastructure/kafka/kafka-cluster.yaml
```

Tìm và giải thích:

- label liên kết NodePool với Kafka cluster;
- hai annotation bật node pools và KRaft;
- số replica;
- hai role của mỗi node;
- listener nội bộ;
- lý do storage hiện tại chưa phù hợp production.

### Bài tập 4: Quan sát self-healing

1. Lưu output `kubectl get pods -n kafka -o wide` trước khi xóa Pod.
2. Xóa một Kafka Pod.
3. Theo dõi Pod mới.
4. Ghi lại sự khác nhau giữa tên Pod, thời gian tạo và trạng thái.
5. Giải thích vì sao việc Pod được tạo lại không chứng minh data persistence.

### Bài tập 5: Phân biệt hai loại log

Viết hai ví dụ cho mỗi loại:

```text
Data log:
Metadata log:
```

Đặc biệt ghi rõ KRaft metadata log không phải nơi lưu toàn bộ record của topic.

## 18. Checklist hoàn thành Phase 3

- [ ] Đã kiểm tra Minikube node và Strimzi Operator.
- [ ] Đã kiểm tra API version của `Kafka` và `KafkaNodePool` CRD.
- [ ] Đã xác minh Kafka version/metadata version được Operator hỗ trợ.
- [ ] Đã hiểu KRaft không dùng ZooKeeper.
- [ ] Đã hiểu broker, controller, dual-role node và controller quorum.
- [ ] Đã kiểm tra `KafkaNodePool` có ba replica.
- [ ] Đã kiểm tra roles gồm `controller` và `broker`.
- [ ] Đã apply KafkaNodePool và Kafka CR đúng thứ tự.
- [ ] Kafka CR `kafka-cluster` tồn tại.
- [ ] KafkaNodePool `kafka-pool` tồn tại.
- [ ] Ba Kafka node Pod đạt trạng thái sẵn sàng.
- [ ] Đã kiểm tra Service, ConfigMap, Secret và Events.
- [ ] Đã quan sát Operator reconciliation/log.
- [ ] Đã mô phỏng xóa một Pod khi cluster chưa có dữ liệu quan trọng.
- [ ] Hiểu rằng Minikube một node không phải HA production.

## 19. Kết nối sang Phase 4

Phase 3 dùng:

```yaml
storage:
  type: ephemeral
```

Phase 4 sẽ tập trung vào persistence:

```text
Kafka data
    |
    v
PersistentVolumeClaim
    |
    v
PersistentVolume
    |
    v
KafkaNodePool storage
    |
    v
Pod restart/recreation
    |
    v
Kiểm tra dữ liệu còn tồn tại
```

Trước khi chuyển phase, nên lưu lại baseline:

```bash
kubectl get kafka -n kafka -o yaml > /tmp/phase-3-kafka.yaml
kubectl get kafkanodepool -n kafka -o yaml > /tmp/phase-3-nodepool.yaml
kubectl get pods -n kafka -o wide
kubectl get svc -n kafka
```

Các file trong `/tmp` chỉ là snapshot phục vụ học tập; cấu hình chính thức vẫn nằm trong manifest của repository.