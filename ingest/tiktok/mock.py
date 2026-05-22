import uuid
import random
import hashlib
from datetime import datetime, timedelta
from ..utils.minio_client import MinioClient
from ..utils.kafka_producer import KafkaJsonProducer

class MockGenerator:
    """
    TikTok Ads Mock Data Generator.
    Deterministic, hierarchical, and realistic metric simulation.
    """

    NAME_POOLS = {
        "campaign": [
            "Seeding Camp Hà Linh", "Brand Awareness - TikTok Video",
            "TikTok Shop - Livestream Mùa Hè 2026", "Tương tác Video - Trang sức Cara Luna",
            "Chuyển đổi - Mua sắm tại chỗ", "Khuyến mãi 30/4 - TikTok Ads"
        ],
        "adgroup": [
            "V_seeding_HL_280326", "V_CNL_SeedingHL_SauLive_210326", "Tệp Re-marketing 30d",
            "Đối tượng Quan tâm làm đẹp", "Sở thích Thời trang và Trang sức", "Lookalike 1% Buyers"
        ],
        "ad_text": [
            "Note ngay vào list những món phải săn cho chồng nhé các vợ iuu #halinhofficial #vohalinh #cnewlab #livestream #listdeal",
            "Tình hình kho hàng của C'New Lab sau phiên live của Chiến thần Võ Hà Linh! Cảm ơn khách iu đã luôn tin tưởng và ủng hộ chúng mình ạ! #cnewlab #xuhuong #fyp #halinhofficial #superlive",
            "Quà Tặng Cho Bạn Gái Mới Yêu #caraluna #quatang",
            "Giảm Giá 20% Cho Đơn Đầu Tiên #khuyenmai #giamgia",
            "Cara Luna - Đánh Thức Vẻ Đẹp #trangsuc #baccaocap"
        ],
        "ad_name": [
            "V_seeding_HL_heittienchamda_280326", "Copy 1 of V_CNL_SeedingHL_SauLive_210326",
            "Video Quay Sản Phẩm Mới", "KOL Review Dây Chuyền Bạc", "Livestream Highlight"
        ],
        "advertiser_name": [
            "Kascom - C' New Lab 1", "Cara Luna Official", "Tiktok Shop Demo Advertiser"
        ]
    }

    def __init__(self, endpoint=None, access_key=None, secret_key=None, enable_xlsx_buffer=False, output_mode="minio", kafka_bootstrap_servers=None):
        self.output_mode = output_mode
        self.minio_client = MinioClient(endpoint, access_key, secret_key) if output_mode == "minio" else None
        self.kafka_producer = KafkaJsonProducer(kafka_bootstrap_servers) if output_mode == "kafka" else None
        self.rng = random.Random()
        self.export_buffer = {}
        self.enable_xlsx_buffer = enable_xlsx_buffer

    def _get_deterministic_id(self, seed_str, prefix="", is_int=False):
        h = hashlib.md5(seed_str.encode()).hexdigest()
        if is_int:
            return int(h[:15], 16)
        if prefix:
            return f"{prefix}_{h[:12]}"
        return str(int(h[:12], 16))[:12]

    def _get_random_name(self, category, seed_str):
        local_rng = random.Random(seed_str)
        pool = self.NAME_POOLS.get(category, ["Default Name"])
        base = local_rng.choice(pool)
        return f"{base}"

    def _get_dates_in_range(self, start_date_str, end_date_str):
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        dates = []
        current = start_date
        while current <= end_date:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        return dates

    def _get_seasonality_multiplier(self, date_str):
        p_date = datetime.strptime(date_str, "%Y-%m-%d")
        day_of_week = p_date.weekday()
        multiplier = 1.0
        if day_of_week >= 5: multiplier = 1.4 # Weekend spikes on Tiktok
        month, day = p_date.month, p_date.day
        if month == 2 and 10 <= day <= 14: multiplier *= 2.5
        if month == 3 and 5 <= day <= 8: multiplier *= 2.0
        return multiplier

    def _upload_chunk(self, table_name, data):
        if not data: return
        if self.enable_xlsx_buffer:
            if table_name not in self.export_buffer: self.export_buffer[table_name] = []
            self.export_buffer[table_name].extend(data)
        if self.output_mode == "kafka":
            self.kafka_producer.produce(table_name, data)
        else:
            result = self.minio_client.upload_json(table_name, data)
            if not result["success"]: print(f"   ... FAILED upload {table_name}: {result['error']}")

    def export_to_xlsx(self, filepath):
        try:
            import pandas as pd
            if not self.export_buffer: return
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                for table_name, records in self.export_buffer.items():
                    sheet_name = table_name.replace("tta_", "")[:31]
                    pd.DataFrame(records).to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"Mock Generator (Tiktok): Exported to {filepath}")
        except Exception as e: print(f"Export error: {e}")

    def generate_consistent_suite(self, start_date, end_date, user_id="cmncpvt100052lb013n2j4qf4", options=None):
        options = options or {}
        acc_cnt = options.get('accountCount', 1)
        cam_per_acc = options.get('campaignCount', 2)
        group_per_cam = options.get('adGroupCount', 2)
        ad_per_group = options.get('adCount', 2)
        base_seed = options.get('seed', 'tiktok_ads_seed_2026')

        self.export_buffer = {}
        dates = self._get_dates_in_range(start_date, end_date)

        # Build hierarchy
        accounts = []
        for i in range(acc_cnt):
            acc_seed = f"{base_seed}_acc_{i}"
            acc = {
                "id": self._get_deterministic_id(acc_seed, is_int=True),
                "name": self._get_random_name("advertiser_name", acc_seed),
                "campaigns": []
            }

            for j in range(cam_per_acc):
                cam_seed = f"{acc_seed}_cam_{j}"
                cam = {
                    "id": self._get_deterministic_id(cam_seed, is_int=True),
                    "name": self._get_random_name("campaign", cam_seed),
                    "ad_groups": []
                }

                for k in range(group_per_cam):
                    grp_seed = f"{cam_seed}_grp_{k}"
                    grp_rng = random.Random(grp_seed)

                    # Group lifecycle
                    start_offset = grp_rng.randint(-10, 10)
                    duration = grp_rng.randint(30, 90)

                    base_date = datetime.strptime(start_date, "%Y-%m-%d")
                    grp_start = (base_date + timedelta(days=start_offset)).strftime("%Y-%m-%d")
                    grp_end = (base_date + timedelta(days=start_offset + duration)).strftime("%Y-%m-%d")

                    group = {
                        "id": self._get_deterministic_id(grp_seed, is_int=True),
                        "name": self._get_random_name("adgroup", grp_seed),
                        "budget": grp_rng.randint(500000, 2000000),
                        "start_date": grp_start,
                        "end_date": grp_end,
                        "ads": []
                    }

                    for l in range(ad_per_group):
                        ad_seed = f"{grp_seed}_ad_{l}"
                        ad_rng = random.Random(ad_seed)
                        group["ads"].append({
                            "id": self._get_deterministic_id(ad_seed, is_int=True),
                            "name": self._get_random_name("ad_name", ad_seed),
                            "text": self._get_random_name("ad_text", ad_seed),
                            "quality_score": ad_rng.uniform(0.8, 1.2)
                        })
                    cam["ad_groups"].append(group)
                acc["campaigns"].append(cam)
            accounts.append(acc)

        print("Mock Generator (Tiktok): Built hierarchy.")

        TABLE_NAME = "TTA_ad_performance"
        day_buffer = []

        now = datetime.now()

        # Generate Data
        for date in dates:
            print(f"   Processing Date: {date}...")
            seasonality = self._get_seasonality_multiplier(date)

            for acc in accounts:
                for cam in acc["campaigns"]:
                    for grp in cam["ad_groups"]:
                        # check if group is active
                        if not (grp["start_date"] <= date <= grp["end_date"]): continue

                        for ad in grp["ads"]:
                            ad_daily_seed = f"{ad['id']}_{date}"
                            ad_rng = random.Random(ad_daily_seed)

                            curr_dt = datetime.strptime(date, "%Y-%m-%d")
                            start_dt = datetime.strptime(grp["start_date"], "%Y-%m-%d")
                            days_active = max(0, (curr_dt - start_dt).days)
                            growth_mult = 1.0 + (days_active * 0.005)

                            intra_day_factor = 1.0
                            if date == now.strftime("%Y-%m-%d"):
                                seconds_passed = now.hour * 3600 + now.minute * 60 + now.second
                                intra_day_factor = seconds_passed / 86400.0

                            base_spend = ad_rng.uniform(50000, 300000)
                            daily_spend = base_spend * growth_mult * intra_day_factor * seasonality

                            base_cpc = ad_rng.uniform(1000, 5000)
                            cpc = base_cpc / ad["quality_score"]

                            clicks = int(daily_spend / cpc)
                            ctr = ad_rng.uniform(0.01, 0.05)
                            impressions = int(clicks / ctr) if ctr > 0 else 0
                            cpm = (daily_spend / impressions * 1000) if impressions > 0 else 0

                            reach = int(impressions / ad_rng.uniform(1.0, 1.5))
                            frequency = impressions / reach if reach > 0 else 1.0

                            # Conversions
                            conversion_rate = ad_rng.uniform(0.01, 0.08)
                            conversions = int(clicks * conversion_rate)
                            cost_per_conversion = daily_spend / conversions if conversions > 0 else 0

                            # Tiktok specific metrics
                            video_play_actions = int(impressions * ad_rng.uniform(0.3, 0.8))
                            profile_visits = int(clicks * ad_rng.uniform(0.1, 0.3))
                            likes = int(video_play_actions * ad_rng.uniform(0.05, 0.15))
                            comments = int(likes * ad_rng.uniform(0.01, 0.1))
                            shares = int(likes * ad_rng.uniform(0.05, 0.2))
                            follows = int(profile_visits * ad_rng.uniform(0.1, 0.4))
                            live_views = int(impressions * ad_rng.uniform(0, 0.2))

                            # Shopping
                            onsite_shopping = int(conversions * ad_rng.uniform(0.5, 1.0))
                            purchase = onsite_shopping
                            cost_per_onsite_shopping = daily_spend / onsite_shopping if onsite_shopping > 0 else 0
                            avg_order_value = ad_rng.uniform(150000, 500000)
                            total_onsite_shopping_value = onsite_shopping * avg_order_value
                            onsite_shopping_roas = total_onsite_shopping_value / daily_spend if daily_spend > 0 else 0

                            row = {
                                "pkId": str(uuid.UUID(int=ad_rng.getrandbits(128), version=4)),
                                "user_id": user_id,
                                "createdAt": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                                "stat_time_day": f"{date} 17:00:00.000",
                                "ad_id": str(ad["id"]),
                                "start_date": f"{grp['start_date']} 00:00:00.000",
                                "end_date": f"{grp['end_date']} 00:00:00.000",
                                "advertiser_id": str(acc["id"]),
                                "advertiser_name": acc["name"],
                                "campaign_name": cam["name"],
                                "adgroup_name": grp["name"],
                                "ad_text": ad["text"],
                                "ad_name": ad["name"],
                                "spend": round(daily_spend, 1),
                                "impressions": impressions,
                                "clicks": clicks,
                                "ctr": round(ctr, 3),
                                "cpc": round(cpc, 1),
                                "cpm": round(cpm, 1),
                                "reach": reach,
                                "frequency": round(frequency, 2),
                                "conversion": conversions,
                                "cost_per_conversion": round(cost_per_conversion, 1),
                                "conversion_rate": round(conversion_rate, 3),
                                "video_play_actions": video_play_actions,
                                "purchase": purchase,
                                "profile_visits": profile_visits,
                                "likes": likes,
                                "comments": comments,
                                "shares": shares,
                                "follows": follows,
                                "live_views": live_views,
                                "total_onsite_shopping_value": round(total_onsite_shopping_value, 1),
                                "onsite_shopping": onsite_shopping,
                                "onsite_shopping_roas": round(onsite_shopping_roas, 2),
                                "cost_per_onsite_shopping": round(cost_per_onsite_shopping, 1),
                                "updatedAt": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                            }
                            day_buffer.append(row)

            # Tiktok typically pushes all data to a single table TTA_ad_performance
            # We will upload chunk by day
            self._upload_chunk(TABLE_NAME, day_buffer)
            day_buffer = [] # clear buffer for next day

        if self.output_mode == "kafka" and self.kafka_producer:
            self.kafka_producer.flush()

        print("Mock Generator (Tiktok): All dates processed.")
