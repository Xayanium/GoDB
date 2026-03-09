package shardctrler

import (
	"GoDB/rpc/shardctrler"
	"sort"
)

type CtrlerStateMachine struct {
	Configs []*ctrlerrpc.Config // stateMachine 存储的配置列表，configs[i] 代表编号为 i 的配置（分片分配方案）
}

func NewCtrlerStateMachine() *CtrlerStateMachine {
	cfg := &CtrlerStateMachine{}
	cfg.Configs = make([]*ctrlerrpc.Config, 1)
	cfg.Configs[0] = &ctrlerrpc.Config{}
	cfg.Configs[0].Groups = make(map[int32]*ctrlerrpc.ServerList)
	cfg.Configs[0].Shards = make([]int32, NShards)
	return cfg
}

// Query 根据版本号 num，获取相关配置，如果 num超出范围(如-1等)则返回最新配置
func (csm *CtrlerStateMachine) Query(num int) (*ctrlerrpc.Config, Err) {
	if num < 0 || num >= len(csm.Configs) {
		return csm.Configs[len(csm.Configs)-1], OK
	}
	return csm.Configs[num], OK
}

// Join 将新的服务节点加入指定的 replica group，并重新均衡 shards 到各个 replica group
func (csm *CtrlerStateMachine) Join(groups map[int][]string) Err {
	lastConf := csm.Configs[len(csm.Configs)-1]
	newConf := getNewConf(lastConf) // 从 lastConf 复制一个配置，并在这个新配置上修改，最后将这个新配置追加到配置列表中

	// 新的 servers 加入到对应的 replica group 中
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		if serverList, ok := newConf.Groups[int32(gid)]; ok {
			serverList.Servers = append(serverList.Servers, newServers...)
		} else {
			newConf.Groups[int32(gid)] = &ctrlerrpc.ServerList{Servers: newServers}
		}
	}

	// 注意：Join 中构造 gid -> shards 需要在 newConfig.Groups 中添加新的 replica group 后进行，使 shards 可迁移到新 group

	// 将 shard -> gid 映射构造为 gid -> shards 映射，方便进行 groups 间的 shard 迁移
	gid2Shards := getGid2Shards(newConf)

	// 负载均衡：将 shards 均匀分配到新配置的各个 replica group 中（Join可能引入新的 replica group）
	shardRebalance(gid2Shards)

	// 将负载均衡后的 gid -> shard 映射转换为 shard -> gid 映射，存储在 newConf.Shards 中
	newConf.Shards = getShard2Gid(gid2Shards)

	csm.Configs = append(csm.Configs, newConf)
	return OK
}

// Leave 移除指定 gid 的 replica groups，原属于它们的 shard 重新分配给剩余的 replica groups
func (csm *CtrlerStateMachine) Leave(gids []int) Err {
	lastConf := csm.Configs[len(csm.Configs)-1]
	newConf := getNewConf(lastConf)

	// 注意：Move 中构造 gid -> shards 需要在 newConfig.Groups 中删除指定 gid 的 replica group 前进行，已保留删除 group 的 shards 信息

	// 将 shard -> gid 映射构造为 gid -> shards 映射，方便进行 groups 间的 shard 迁移
	gid2Shards := getGid2Shards(newConf)

	// 删除newConf中指定 gid 的 replica groups，将被删除 groups 对应的 shards 重新分配到其他 groups
	for _, gid := range gids {
		if gid == 0 {
			continue
		}

		// 删除 newConf 中指定 gid 的 replica group
		if _, ok := newConf.Groups[int32(gid)]; ok {
			delete(newConf.Groups, int32(gid))
		}

		// 将被删除 groups 对应的 shards 追加到 gid=0 的 shard 列表中，表示这些 shard 还未分配，等待 rebalance 重新分配
		if _, ok := gid2Shards[gid]; ok {
			gid2Shards[0] = append(gid2Shards[0], gid2Shards[gid]...)
			delete(gid2Shards, gid)
		}
	}

	// 负载均衡：将 shards 均匀分配到新配置的各个 replica group 中（Leave 删除 group 后将原 group 的 shard 重新分配）
	shardRebalance(gid2Shards)

	// 将负载均衡后的 gid -> shard 映射转换为 shard -> gid 映射，存储在 newConf.Shards 中
	newConf.Shards = getShard2Gid(gid2Shards)

	csm.Configs = append(csm.Configs, newConf)
	return OK
}

// Move 手动指定某个 shard 分配给某个 gid（不做负载均衡，只改变当前 shard 的归属）
func (csm *CtrlerStateMachine) Move(shard, gid int) Err {
	lastConf := csm.Configs[len(csm.Configs)-1]
	newConf := getNewConf(lastConf)

	newConf.Shards[shard] = int32(gid)

	csm.Configs = append(csm.Configs, newConf)
	return OK
}

// ============================================================================
// 辅助函数
// ============================================================================

// 从stateMachine已有的最后一个 lastConfig 中复制得到一个新配置
func getNewConf(lastConf *ctrlerrpc.Config) *ctrlerrpc.Config {
	newConf := &ctrlerrpc.Config{}
	// 配置的版本号+1
	newConf.Num = lastConf.Num + 1
	// 复制 lastConfig 的 shards 分片方案（第 i 号分片由 gid=shards[i] 这个 replica group 负责读写）
	newConf.Shards = make([]int32, NShards)
	copy(newConf.Shards, lastConf.Shards)
	// 复制 lastConfig 中每个 gid 对应的 raft 组里有的服务器
	newConf.Groups = make(map[int32]*ctrlerrpc.ServerList)
	for gid, servers := range lastConf.Groups {
		newServers := make([]string, len(servers.Servers))
		copy(newServers, servers.Servers)
		newConf.Groups[gid] = &ctrlerrpc.ServerList{Servers: newServers}
	}
	return newConf
}

// 转换前：
// shard   gid
//
//	0      1
//	1      1
//	2      2
//	3      2
//	4      1
//
// 转换后：
//
//	gid       shard
//	 1       [0, 1, 4]
//	 2       [2, 3]
func getGid2Shards(conf *ctrlerrpc.Config) map[int][]int {
	gid2Shards := make(map[int][]int)
	for gid, _ := range conf.Groups {
		gid2Shards[int(gid)] = make([]int, 0)
	}
	for shard, gid := range conf.Shards {
		gid2Shards[int(gid)] = append(gid2Shards[int(gid)], shard)
	}
	return gid2Shards
}

// 转换前：
//
//	gid       shard
//	 1       [0, 1, 4]
//	 2       [2, 3]
//
// 转换后：
// shard   gid
//
//	0      1
//	1      1
//	2      2
//	3      2
//	4      1
func getShard2Gid(gid2Shards map[int][]int) []int32 {
	shards := make([]int32, NShards)
	for gid, shardList := range gid2Shards {
		for _, shard := range shardList {
			shards[shard] = int32(gid)
		}
	}
	return shards
}

// 将 shards 均匀分配到新配置的各个 replica group 中（Join可能引入新的 replica group）
// 每次循环找到当前 shard 最多的 gid（max_gid）、shard 最少的 gid（min_gid），如果两者之间 shard 相差大于1，进行 shard 迁移
// 从 max_gid 中拿第一个 shard，追加给 min_gid，并删除 max_gid 中的这个 shard，直到 max_gid 和 min_gid 之间 shard 相差不大于1
// 示例：
//
//	gid         shard
//	 1	     [0, 1, 4, 6]
//	 2	     [2, 3]
//	 3	     [5, 7]
//	 4	     []
//
// ------------ 第1次移动 --------------
//
//	1	     [1, 4, 6]
//	2	     [2, 3]
//	3	     [5, 7]
//	4	     [0]
//
// ------------ 第2次移动 --------------
//
//	1	     [4, 6]
//	2	     [2, 3]
//	3	     [5, 7]
//	4	     [0, 1]
func shardRebalance(gid2Shards map[int][]int) {
	// 当没有有效的 Replica Group，或者只剩下 group 0 时，不需要 rebalance
	if len(gid2Shards) == 0 || (len(gid2Shards) == 1 && len(gid2Shards[0]) > 0) {
		return
	}

	for {
		maxGid, minGid := getMaxShardGid(gid2Shards), getMinShardGid(gid2Shards)
		// 如果 gid=0 存在 shard，表示未分配的 shard，Join 中会优先分配这些 shard
		// max_gid 和 min_gid 之间 shard 相差不大于1，结束负载均衡
		if maxGid != 0 && len(gid2Shards[maxGid])-len(gid2Shards[minGid]) <= 1 {
			break
		}
		// 否则从 max_gid 中拿第一个 shard，追加给 min_gid，并删除 max_gid 中的这个 shard
		gid2Shards[minGid] = append(gid2Shards[minGid], gid2Shards[maxGid][0])
		gid2Shards[maxGid] = gid2Shards[maxGid][1:]
	}
}

func getMaxShardGid(gid2Shards map[int][]int) int {
	// 如果 gid_0 存在 shard，表示未分配的 shard，会优先分配这些 shard
	if shards, ok := gid2Shards[0]; ok && len(shards) > 0 {
		return 0
	}

	// 由于 golang 遍历 map 的顺序是随机的，我们需要按 gid 升序遍历 gidToShards，防止因存在多个数量相同的 最大/最小 shards 数组，造成函数每次获取到的 gid 号不同
	gids := make([]int, 0)
	for gid, _ := range gid2Shards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	maxGid, maxNum := -1, -1
	for _, gid := range gids {
		if len(gid2Shards[gid]) > maxNum {
			maxGid, maxNum = gid, len(gid2Shards[gid])
		}
	}
	return maxGid
}

func getMinShardGid(gid2Shards map[int][]int) int {
	gids := make([]int, 0)
	for gid, _ := range gid2Shards {
		// group 0 不能在负载均衡时被分配到 shard
		if gid == 0 {
			continue
		}
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	minGid, minNum := -1, NShards+1
	for _, gid := range gids {
		if len(gid2Shards[gid]) < minNum {
			minGid, minNum = gid, len(gid2Shards[gid])
		}
	}
	return minGid
}
