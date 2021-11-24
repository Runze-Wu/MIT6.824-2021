# MIT 6.824-2021

## 实现

- [x] Lab1

- [x] Lab2
  - [x] Lab2A
  - [x] Lab2B
  - [x] Lab2C
  - [x] Lab2D ~~时间较慢，可能会影响Lab3、Lab4的测试，尚需改进。~~
    - applier改为使用信号量去唤醒而不使用channel唤醒，channel在信道满时存在阻塞情况；
    - 在Leader和Follower更新commitIndex时，均唤醒applier，发送速度显著提升。
  
- [ ] Lab3
  - [x] Lab3A
  
  - [x] Lab3B
  
    存在不可串行化的情况，还未找到原因。
  
- [ ] Lab4