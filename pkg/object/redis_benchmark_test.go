//go:build !noredis
// +build !noredis

/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package object

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestRedisListAllIncludesEmptyObjects(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("set REDIS_ADDR to a dedicated Redis instance")
	}

	storage, err := newRedis(addr, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	rdb := storage.(*redisStore).rdb
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	prefix := fmt.Sprintf("__juicefs_test_listall_empty_%d_%d/", os.Getpid(), time.Now().UnixNano())
	emptyKey, nonemptyKey := prefix+"empty", prefix+"nonempty"
	t.Cleanup(func() { _ = rdb.Del(ctx, emptyKey, nonemptyKey).Err() })
	if err := rdb.Set(ctx, emptyKey, []byte{}, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, nonemptyKey, []byte("data"), 0).Err(); err != nil {
		t.Fatal(err)
	}

	ch, err := storage.ListAll(ctx, prefix, "", false)
	if err != nil {
		t.Fatal(err)
	}
	var got []Object
	for object := range ch {
		if object == nil {
			t.Fatal("ListAll returned an incomplete listing")
		}
		got = append(got, object)
	}
	if len(got) != 2 || got[0].Key() != emptyKey || got[0].Size() != 0 || got[1].Key() != nonemptyKey || got[1].Size() != 4 {
		t.Fatalf("unexpected listing: %+v", got)
	}
}

func BenchmarkRedisListAll(b *testing.B) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		b.Skip("set REDIS_ADDR to a dedicated Redis instance")
	}

	const objectCount = 10000
	for _, tc := range []struct {
		name         string
		emptyPercent int
	}{
		{name: "nonempty", emptyPercent: 0},
		{name: "half_empty", emptyPercent: 50},
		{name: "empty", emptyPercent: 100},
	} {
		b.Run(tc.name, func(b *testing.B) {
			storage, err := newRedis(addr, "", "", "")
			if err != nil {
				b.Fatal(err)
			}
			rdb := storage.(*redisStore).rdb
			ctx := context.Background()
			if err := rdb.Ping(ctx).Err(); err != nil {
				_ = rdb.Close()
				b.Fatal(err)
			}

			prefix := fmt.Sprintf("__juicefs_benchmark_listall_%d_%s/", os.Getpid(), tc.name)
			keys := make([]string, 0, objectCount)
			for start := 0; start < objectCount; start += 1000 {
				end := start + 1000
				if end > objectCount {
					end = objectCount
				}
				pipe := rdb.Pipeline()
				for i := start; i < end; i++ {
					key := fmt.Sprintf("%s%08d", prefix, i)
					keys = append(keys, key)
					var value []byte = []byte("benchmark-payload")
					if i%100 < tc.emptyPercent {
						value = []byte{}
					}
					pipe.Set(ctx, key, value, 0)
				}
				if _, err := pipe.Exec(ctx); err != nil {
					_ = rdb.Close()
					b.Fatal(err)
				}
			}
			b.Cleanup(func() {
				for start := 0; start < len(keys); start += 1000 {
					end := start + 1000
					if end > len(keys) {
						end = len(keys)
					}
					pipe := rdb.Pipeline()
					pipe.Del(ctx, keys[start:end]...)
					_, _ = pipe.Exec(ctx)
				}
				_ = rdb.Close()
			})

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ch, err := storage.ListAll(ctx, prefix, "", false)
				if err != nil {
					b.Fatal(err)
				}
				count := 0
				for obj := range ch {
					if obj == nil {
						b.Fatal("ListAll returned an incomplete listing")
					}
					count++
				}
				if count != objectCount {
					b.Fatalf("ListAll returned %d objects, want %d", count, objectCount)
				}
			}
			b.StopTimer()
			if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
				b.ReportMetric(float64(objectCount*b.N)/elapsed, "objects/s")
			}
		})
	}
}
