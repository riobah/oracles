use crate::{
    coverage::{CoverageReward, CoveredHexStream, CoveredHexes},
    data_session::{HotspotMap, ServiceProviderDataSession},
    heartbeats::{HeartbeatReward, OwnedKeyType},
    speedtests_average::{SpeedtestAverage, SpeedtestAverages},
    subscriber_location::SubscriberValidatedLocations,
};
use chrono::{DateTime, Duration, Utc};
use file_store::traits::TimestampEncode;
use futures::{Stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::{
        poc_mobile as proto,
        poc_mobile::{
            mobile_reward_share::Reward as ProtoReward, UnallocatedReward, UnallocatedRewardType,
        },
    },
    ServiceProvider,
};
use mobile_config::{
    boosted_hex_info::{BoostedHex, BoostedHexes},
    client::{carrier_service_client::CarrierServiceVerifier, ClientError},
};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use std::{collections::HashMap, ops::Range};
use uuid::Uuid;

/// Total tokens emissions pool per 365 days or 366 days for a leap year
const TOTAL_EMISSIONS_POOL: Decimal = dec!(30_000_000_000_000_000);

/// Maximum amount of the total emissions pool allocated for data transfer
/// rewards
const MAX_DATA_TRANSFER_REWARDS_PERCENT: Decimal = dec!(0.4);

/// The fixed price of a mobile data credit
const DC_USD_PRICE: Decimal = dec!(0.00001);

/// Default precision used for rounding
const DEFAULT_PREC: u32 = 15;

/// Percent of total emissions allocated for mapper rewards
const MAPPERS_REWARDS_PERCENT: Decimal = dec!(0.2);

/// shares of the mappers pool allocated per eligible subscriber for discovery mapping
const DISCOVERY_MAPPING_SHARES: Decimal = dec!(30);

// Percent of total emissions allocated for service provider rewards
const SERVICE_PROVIDER_PERCENT: Decimal = dec!(0.1);

// Percent of total emissions allocated for oracles
const ORACLES_PERCENT: Decimal = dec!(0.04);

#[derive(Debug)]
pub struct TransferRewards {
    reward_scale: Decimal,
    rewards: HashMap<PublicKeyBinary, TransferReward>,
    reward_sum: Decimal,
    mobile_bone_price: Decimal,
}

#[derive(Copy, Clone, Debug)]
pub struct TransferReward {
    bones: Decimal,
    bytes_rewarded: u64,
}

impl TransferRewards {
    pub fn reward_scale(&self) -> Decimal {
        self.reward_scale
    }

    pub fn reward_sum(&self) -> Decimal {
        self.reward_sum
    }

    #[cfg(test)]
    fn reward(&self, hotspot: &PublicKeyBinary) -> Decimal {
        self.rewards
            .get(hotspot)
            .copied()
            .map(|x| x.bones)
            .unwrap_or(Decimal::ZERO)
            * self.reward_scale
    }

    pub fn total(&self) -> Decimal {
        self.rewards
            .values()
            .map(|v| v.bones * self.reward_scale)
            .sum()
    }

    pub async fn from_transfer_sessions(
        mobile_bone_price: Decimal,
        transfer_sessions: HotspotMap,
        epoch: &Range<DateTime<Utc>>,
    ) -> Self {
        let mut reward_sum = Decimal::ZERO;
        let rewards = transfer_sessions
            .into_iter()
            // Calculate rewards per hotspot
            .map(|(pub_key, rewardable)| {
                let bones =
                    dc_to_mobile_bones(Decimal::from(rewardable.rewardable_dc), mobile_bone_price);
                reward_sum += bones;
                (
                    pub_key,
                    TransferReward {
                        bones,
                        bytes_rewarded: rewardable.rewardable_bytes,
                    },
                )
            })
            .collect();

        let duration = epoch.end - epoch.start;
        let total_emissions_pool = get_total_scheduled_tokens(duration);

        // Determine if we need to scale the rewards given for data transfer rewards.
        // Ideally this should never happen, but if the total number of data transfer rewards
        // is greater than (at the time of writing) 40% of the total pool, we need to scale
        // the rewards given for data transfer.
        //
        // If we find that total data_transfer reward sum is greater than 40%, we use the
        // following math to calculate the scale:
        //
        // [ scale * data_transfer_reward_sum ] / total_emissions_pool = 0.4
        //
        //   therefore:
        //
        // scale = [ 0.4 * total_emissions_pool ] / data_transfer_reward_sum
        //
        let reward_scale = if reward_sum / total_emissions_pool > MAX_DATA_TRANSFER_REWARDS_PERCENT
        {
            MAX_DATA_TRANSFER_REWARDS_PERCENT * total_emissions_pool / reward_sum
        } else {
            Decimal::ONE
        };

        Self {
            reward_scale,
            rewards,
            reward_sum: reward_sum * reward_scale,
            mobile_bone_price,
        }
    }

    pub fn into_rewards(
        self,
        epoch: &'_ Range<DateTime<Utc>>,
    ) -> impl Iterator<Item = (u64, proto::MobileRewardShare)> + '_ {
        let Self {
            reward_scale,
            rewards,
            ..
        } = self;
        let start_period = epoch.start.encode_timestamp();
        let end_period = epoch.end.encode_timestamp();
        rewards
            .into_iter()
            .map(move |(hotspot_key, reward)| {
                let dc_transfer_reward = (reward.bones * reward_scale)
                    .round_dp_with_strategy(0, RoundingStrategy::ToZero)
                    .to_u64()
                    .unwrap_or(0);
                (
                    dc_transfer_reward,
                    proto::MobileRewardShare {
                        start_period,
                        end_period,
                        reward: Some(proto::mobile_reward_share::Reward::GatewayReward(
                            proto::GatewayReward {
                                hotspot_key: hotspot_key.into(),
                                dc_transfer_reward,
                                rewardable_bytes: reward.bytes_rewarded,
                                price: (self.mobile_bone_price * dec!(1_000_000) * dec!(1_000_000))
                                    .to_u64()
                                    .unwrap_or_default(),
                            },
                        )),
                    },
                )
            })
            .filter(|(dc_transfer_reward, _mobile_reward)| *dc_transfer_reward > 0)
    }
}

#[derive(Default)]
pub struct MapperShares {
    pub discovery_mapping_shares: SubscriberValidatedLocations,
}

impl MapperShares {
    pub fn new(discovery_mapping_shares: SubscriberValidatedLocations) -> Self {
        Self {
            discovery_mapping_shares,
        }
    }

    pub fn rewards_per_share(&self, total_mappers_pool: Decimal) -> anyhow::Result<Decimal> {
        // note: currently rewards_per_share calculation only takes into
        // consideration discovery mapping shares
        // in the future it will also need to take into account
        // verification mapping shares
        // the number of subscribers eligible for discovery location rewards
        let discovery_mappers_count = Decimal::from(self.discovery_mapping_shares.len());

        // calculate the total eligible mapping shares for the epoch
        // this could be simplified as every subscriber is awarded the same share
        // however the function is setup to allow the verification mapper shares to be easily
        // added without impacting code structure ( the per share value for those will be different )
        let total_mapper_shares = discovery_mappers_count * DISCOVERY_MAPPING_SHARES;
        let res = total_mappers_pool
            .checked_div(total_mapper_shares)
            .unwrap_or(Decimal::ZERO);
        Ok(res)
    }

    pub fn into_subscriber_rewards(
        self,
        reward_period: &'_ Range<DateTime<Utc>>,
        reward_per_share: Decimal,
    ) -> impl Iterator<Item = (u64, proto::MobileRewardShare)> + '_ {
        self.discovery_mapping_shares
            .into_iter()
            .map(move |subscriber_id| proto::SubscriberReward {
                subscriber_id,
                discovery_location_amount: (DISCOVERY_MAPPING_SHARES * reward_per_share)
                    .round_dp_with_strategy(0, RoundingStrategy::ToZero)
                    .to_u64()
                    .unwrap_or_default(),
            })
            .filter(|subscriber_reward| subscriber_reward.discovery_location_amount > 0)
            .map(|subscriber_reward| {
                (
                    subscriber_reward.discovery_location_amount,
                    proto::MobileRewardShare {
                        start_period: reward_period.start.encode_timestamp(),
                        end_period: reward_period.end.encode_timestamp(),
                        reward: Some(ProtoReward::SubscriberReward(subscriber_reward)),
                    },
                )
            })
    }
}

#[derive(Default)]
pub struct ServiceProviderShares {
    pub shares: Vec<ServiceProviderDataSession>,
}

impl ServiceProviderShares {
    pub fn new(shares: Vec<ServiceProviderDataSession>) -> Self {
        Self { shares }
    }

    pub async fn from_payers_dc(
        payer_shares: HashMap<String, u64>,
        client: &impl CarrierServiceVerifier<Error = ClientError>,
    ) -> anyhow::Result<ServiceProviderShares> {
        let mut sp_shares = ServiceProviderShares::default();
        for (payer, total_dcs) in payer_shares {
            let service_provider = Self::payer_key_to_service_provider(&payer, client).await?;
            sp_shares.shares.push(ServiceProviderDataSession {
                service_provider,
                total_dcs: Decimal::from(total_dcs),
            })
        }
        Ok(sp_shares)
    }

    fn total_dc(&self) -> Decimal {
        self.shares.iter().map(|v| v.total_dcs).sum()
    }

    pub fn rewards_per_share(
        &self,
        total_sp_rewards: Decimal,
        mobile_bone_price: Decimal,
    ) -> anyhow::Result<Decimal> {
        // the total amount of DC spent across all service providers
        let total_sp_dc = self.total_dc();
        // the total amount of service provider rewards in bones based on the spent DC
        let total_sp_rewards_used = dc_to_mobile_bones(total_sp_dc, mobile_bone_price);
        // cap the service provider rewards if used > pool total
        let capped_sp_rewards_used =
            Self::maybe_cap_service_provider_rewards(total_sp_rewards_used, total_sp_rewards);
        Ok(Self::calc_rewards_per_share(
            capped_sp_rewards_used,
            total_sp_dc,
        ))
    }

    pub fn into_service_provider_rewards(
        self,
        reward_period: &'_ Range<DateTime<Utc>>,
        reward_per_share: Decimal,
    ) -> impl Iterator<Item = (u64, proto::MobileRewardShare)> + '_ {
        self.shares
            .into_iter()
            .map(move |share| proto::ServiceProviderReward {
                service_provider_id: share.service_provider as i32,
                amount: (share.total_dcs * reward_per_share)
                    .round_dp_with_strategy(0, RoundingStrategy::ToZero)
                    .to_u64()
                    .unwrap_or(0),
            })
            .filter(|service_provider_reward| service_provider_reward.amount > 0)
            .map(|service_provider_reward| {
                (
                    service_provider_reward.amount,
                    proto::MobileRewardShare {
                        start_period: reward_period.start.encode_timestamp(),
                        end_period: reward_period.end.encode_timestamp(),
                        reward: Some(ProtoReward::ServiceProviderReward(service_provider_reward)),
                    },
                )
            })
    }

    pub fn into_unallocated_reward(
        unallocated_amount: Decimal,
        reward_period: &'_ Range<DateTime<Utc>>,
    ) -> anyhow::Result<proto::MobileRewardShare> {
        let reward = UnallocatedReward {
            reward_type: UnallocatedRewardType::ServiceProvider as i32,
            amount: unallocated_amount
                .round_dp_with_strategy(0, RoundingStrategy::ToZero)
                .to_u64()
                .unwrap_or(0),
        };
        Ok(proto::MobileRewardShare {
            start_period: reward_period.start.encode_timestamp(),
            end_period: reward_period.end.encode_timestamp(),
            reward: Some(ProtoReward::UnallocatedReward(reward)),
        })
    }

    fn maybe_cap_service_provider_rewards(
        total_sp_rewards_used: Decimal,
        total_sp_rewards: Decimal,
    ) -> Decimal {
        match total_sp_rewards_used <= total_sp_rewards {
            true => total_sp_rewards_used,
            false => total_sp_rewards,
        }
    }

    fn calc_rewards_per_share(total_rewards: Decimal, total_shares: Decimal) -> Decimal {
        if total_shares > Decimal::ZERO {
            (total_rewards / total_shares)
                .round_dp_with_strategy(DEFAULT_PREC, RoundingStrategy::MidpointNearestEven)
        } else {
            Decimal::ZERO
        }
    }

    async fn payer_key_to_service_provider(
        payer: &str,
        client: &impl CarrierServiceVerifier<Error = ClientError>,
    ) -> anyhow::Result<ServiceProvider> {
        tracing::info!(payer, "getting service provider for payer");
        let sp = client.payer_key_to_service_provider(payer).await?;
        Ok(sp)
    }
}

/// Returns the equivalent amount of Mobile bones for a specified amount of Data Credits
pub fn dc_to_mobile_bones(dc_amount: Decimal, mobile_bone_price: Decimal) -> Decimal {
    let dc_in_usd = dc_amount * DC_USD_PRICE;
    (dc_in_usd / mobile_bone_price)
        .round_dp_with_strategy(DEFAULT_PREC, RoundingStrategy::ToPositiveInfinity)
}

#[derive(Debug)]
struct RadioPoints {
    location_trust_score_multiplier: Decimal,
    coverage_object: Uuid,
    seniority: DateTime<Utc>,
    points: Decimal,
    // list of all hexes that have been boosted for this hotspot along with the multiplier for each hex
    // this gets included in the radio reward share proto
    boosted_hexes: Vec<BoostedHex>,
}

impl RadioPoints {
    fn new(
        location_trust_score_multiplier: Decimal,
        coverage_object: Uuid,
        seniority: DateTime<Utc>,
    ) -> Self {
        Self {
            location_trust_score_multiplier,
            seniority,
            coverage_object,
            points: Decimal::ZERO,
            boosted_hexes: vec![],
        }
    }

    fn points(&self) -> Decimal {
        (self.location_trust_score_multiplier * self.points).max(Decimal::ZERO)
    }
}

// pub type HotspotBoostedHexes = HashMap<u64, u32>;

#[derive(Debug, Default)]
struct HotspotPoints {
    /// Points are multiplied by the multiplier to get shares.
    /// Multiplier should never be zero.
    speedtest_multiplier: Decimal,
    radio_points: HashMap<Option<String>, RadioPoints>,
}

impl HotspotPoints {
    pub fn add_coverage_entry(
        &mut self,
        radio_key: OwnedKeyType,
        points: Decimal,
        boosted_hex_info: BoostedHex,
    ) {
        let rp = self
            .radio_points
            .get_mut(&radio_key.clone().into_cbsd_id())
            .unwrap();
        // as per hip93, if radio is wifi & the location trust score multiplier is less than 1,
        // then no boost points for you mister
        let final_boost_info =
            if radio_key.is_wifi() && rp.location_trust_score_multiplier < dec!(1) {
                BoostedHex {
                    location: boosted_hex_info.location,
                    multiplier: 1,
                }
            } else {
                boosted_hex_info
            };
        rp.points += points * Decimal::from(final_boost_info.multiplier);
        rp.boosted_hexes.push(final_boost_info);
    }
}

impl HotspotPoints {
    pub fn new(speedtest_multiplier: Decimal) -> Self {
        Self {
            speedtest_multiplier,
            radio_points: HashMap::new(),
        }
    }
}

impl HotspotPoints {
    pub fn total_points(&self) -> Decimal {
        self.speedtest_multiplier
            * self
                .radio_points
                .values()
                .fold(Decimal::ZERO, |sum, radio| sum + radio.points())
    }
}

#[derive(Debug)]
pub struct CoveragePoints {
    coverage_points: HashMap<PublicKeyBinary, HotspotPoints>,
}

impl CoveragePoints {
    pub async fn aggregate_points(
        hex_streams: &impl CoveredHexStream,
        heartbeats: impl Stream<Item = Result<HeartbeatReward, sqlx::Error>>,
        speedtests: &SpeedtestAverages,
        boosted_hexes: &BoostedHexes,
        reward_period: &Range<DateTime<Utc>>,
    ) -> Result<Self, sqlx::Error> {
        let mut heartbeats = std::pin::pin!(heartbeats);
        let mut covered_hexes = CoveredHexes::default();
        let mut coverage_points = HashMap::new();
        while let Some(heartbeat) = heartbeats.next().await.transpose()? {
            let speedtest_multiplier = speedtests
                .get_average(&heartbeat.hotspot_key)
                .as_ref()
                .map_or(Decimal::ZERO, SpeedtestAverage::reward_multiplier);
            let seniority = hex_streams
                .fetch_seniority(heartbeat.key(), reward_period.end)
                .await?;
            let covered_hex_stream = hex_streams
                .covered_hex_stream(heartbeat.key(), &heartbeat.coverage_object, &seniority)
                .await?;
            let overlaps_boosted = covered_hexes
                .aggregate_coverage(&heartbeat.hotspot_key, boosted_hexes, covered_hex_stream)
                .await?;
            let opt_cbsd_id = heartbeat.key().to_owned().into_cbsd_id();
            coverage_points
                .entry(heartbeat.hotspot_key.clone())
                .or_insert_with(|| HotspotPoints::new(speedtest_multiplier))
                .radio_points
                .insert(
                    opt_cbsd_id,
                    RadioPoints::new(
                        heartbeat.trust_score_multiplier(overlaps_boosted),
                        heartbeat.coverage_object,
                        seniority.seniority_ts,
                    ),
                );
        }

        for CoverageReward {
            radio_key,
            points,
            hotspot,
            boosted_hex_info,
        } in covered_hexes.into_coverage_rewards(boosted_hexes, reward_period.start)
        {
            // Guaranteed that points contains the given hotspot.
            coverage_points
                .get_mut(&hotspot)
                .unwrap()
                .add_coverage_entry(radio_key, points, boosted_hex_info)
        }
        Ok(Self { coverage_points })
    }

    /// Only used for testing
    pub fn hotspot_points(&self, hotspot: &PublicKeyBinary) -> Decimal {
        self.coverage_points
            .get(hotspot)
            .map(HotspotPoints::total_points)
            .unwrap_or(Decimal::ZERO)
    }

    pub fn total_shares(&self) -> Decimal {
        self.coverage_points
            .values()
            .fold(Decimal::ZERO, |sum, radio_points| {
                sum + radio_points.total_points()
            })
    }

    pub fn into_rewards(
        self,
        available_poc_rewards: Decimal,
        epoch: &'_ Range<DateTime<Utc>>,
    ) -> Option<impl Iterator<Item = (u64, proto::MobileRewardShare)> + '_> {
        let total_shares = self.total_shares();
        available_poc_rewards
            .checked_div(total_shares)
            .map(|poc_rewards_per_share| {
                tracing::info!(%poc_rewards_per_share);
                let start_period = epoch.start.encode_timestamp();
                let end_period = epoch.end.encode_timestamp();
                self.coverage_points
                    .into_iter()
                    .flat_map(move |(hotspot_key, hotspot_points)| {
                        radio_points_into_rewards(
                            hotspot_key,
                            start_period,
                            end_period,
                            poc_rewards_per_share,
                            hotspot_points.speedtest_multiplier,
                            hotspot_points.radio_points.into_iter(),
                        )
                    })
                    .filter(|(poc_reward, _mobile_reward)| *poc_reward > 0)
            })
    }
}

fn radio_points_into_rewards(
    hotspot_key: PublicKeyBinary,
    start_period: u64,
    end_period: u64,
    poc_rewards_per_share: Decimal,
    speedtest_multiplier: Decimal,
    radio_points: impl Iterator<Item = (Option<String>, RadioPoints)>,
) -> impl Iterator<Item = (u64, proto::MobileRewardShare)> {
    radio_points.map(move |(cbsd_id, radio_points)| {
        new_radio_reward(
            cbsd_id,
            &hotspot_key,
            start_period,
            end_period,
            poc_rewards_per_share,
            speedtest_multiplier,
            radio_points,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn new_radio_reward(
    cbsd_id: Option<String>,
    hotspot_key: &PublicKeyBinary,
    start_period: u64,
    end_period: u64,
    poc_rewards_per_share: Decimal,
    speedtest_multiplier: Decimal,
    radio_points: RadioPoints,
) -> (u64, proto::MobileRewardShare) {
    let poc_reward = poc_rewards_per_share
        * speedtest_multiplier
        * radio_points.location_trust_score_multiplier
        * radio_points.points;
    let hotspot_key: Vec<u8> = hotspot_key.clone().into();
    let cbsd_id = cbsd_id.unwrap_or_default();
    let poc_reward = poc_reward
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0);
    let boosted_hexes = radio_points
        .boosted_hexes
        .iter()
        .filter(|boosted_hex| boosted_hex.multiplier > 1)
        .map(|boosted_hex| proto::BoostedHex {
            location: boosted_hex.location,
            multiplier: boosted_hex.multiplier,
        })
        .collect();
    (
        poc_reward,
        proto::MobileRewardShare {
            start_period,
            end_period,
            reward: Some(proto::mobile_reward_share::Reward::RadioReward(
                proto::RadioReward {
                    hotspot_key,
                    cbsd_id,
                    poc_reward,
                    coverage_points: radio_points.points.to_u64().unwrap_or(0),
                    seniority_timestamp: radio_points.seniority.encode_timestamp(),
                    coverage_object: Vec::from(radio_points.coverage_object.into_bytes()),
                    location_trust_score_multiplier: (radio_points.location_trust_score_multiplier
                        * dec!(1000))
                    .to_u32()
                    .unwrap_or_default(),
                    speedtest_multiplier: (speedtest_multiplier * dec!(1000))
                        .to_u32()
                        .unwrap_or_default(),
                    boosted_hexes,
                    ..Default::default()
                },
            )),
        },
    )
}

pub fn get_total_scheduled_tokens(duration: Duration) -> Decimal {
    (TOTAL_EMISSIONS_POOL / dec!(366) / Decimal::from(Duration::hours(24).num_seconds()))
        * Decimal::from(duration.num_seconds())
}

pub fn get_scheduled_tokens_for_poc(duration: Duration) -> Decimal {
    get_total_scheduled_tokens(duration) * dec!(0.6)
}

pub fn get_scheduled_tokens_for_mappers(duration: Duration) -> Decimal {
    get_total_scheduled_tokens(duration) * MAPPERS_REWARDS_PERCENT
}

pub fn get_scheduled_tokens_for_service_providers(duration: Duration) -> Decimal {
    get_total_scheduled_tokens(duration) * SERVICE_PROVIDER_PERCENT
}

pub fn get_scheduled_tokens_for_oracles(duration: Duration) -> Decimal {
    get_total_scheduled_tokens(duration) * ORACLES_PERCENT
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        cell_type::CellType,
        coverage::{CoveredHexStream, HexCoverage, Seniority},
        data_session::HotspotDataSession,
        data_session::{self, HotspotReward},
        heartbeats::{HeartbeatReward, KeyType, OwnedKeyType},
        reward_shares,
        speedtests::Speedtest,
        speedtests_average::SpeedtestAverage,
        subscriber_location::SubscriberValidatedLocations,
    };
    use chrono::{Duration, Utc};
    use file_store::speedtest::CellSpeedtest;
    use futures::stream::{self, BoxStream};
    use helium_proto::{
        services::poc_mobile::mobile_reward_share::Reward as MobileReward, ServiceProvider,
    };
    use prost::Message;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[test]
    fn ensure_correct_conversion_of_bytes_to_bones() {
        assert_eq!(
            dc_to_mobile_bones(Decimal::from(1), dec!(1.0)),
            dec!(0.00001)
        );
        assert_eq!(
            dc_to_mobile_bones(Decimal::from(2), dec!(1.0)),
            dec!(0.00002)
        );
    }

    fn mobile_bones_to_dc(mobile_bones_amount: Decimal, mobile_bones_price: Decimal) -> Decimal {
        let mobile_value = mobile_bones_amount * mobile_bones_price;
        (mobile_value / DC_USD_PRICE)
            .round_dp_with_strategy(0, RoundingStrategy::ToNegativeInfinity)
    }

    #[tokio::test]
    async fn discover_mapping_amount() {
        // test based on example defined at https://github.com/helium/oracles/issues/422
        // NOTE: the example defined above lists values in mobile tokens, whereas
        //       this test uses mobile bones

        const NUM_SUBSCRIBERS: u64 = 10_000;

        // simulate 10k subscriber location shares
        let mut location_shares = SubscriberValidatedLocations::new();
        for n in 0..NUM_SUBSCRIBERS {
            location_shares.push(n.encode_to_vec());
        }

        // calculate discovery mapping rewards for a 24hr period
        let now = Utc::now();
        let epoch = (now - Duration::hours(24))..now;

        // translate location shares into discovery mapping shares
        let mapping_shares = MapperShares::new(location_shares);
        let total_mappers_pool =
            reward_shares::get_scheduled_tokens_for_mappers(epoch.end - epoch.start);
        let rewards_per_share = mapping_shares
            .rewards_per_share(total_mappers_pool)
            .unwrap();

        // verify total rewards for the epoch
        let total_epoch_rewards = get_total_scheduled_tokens(epoch.end - epoch.start)
            .round_dp_with_strategy(0, RoundingStrategy::ToZero)
            .to_u64()
            .unwrap_or(0);
        assert_eq!(81_967_213_114_754, total_epoch_rewards);

        // verify total rewards allocated to mappers the epoch
        let total_mapper_rewards = get_scheduled_tokens_for_mappers(epoch.end - epoch.start)
            .round_dp_with_strategy(0, RoundingStrategy::ToZero)
            .to_u64()
            .unwrap_or(0);
        assert_eq!(16_393_442_622_950, total_mapper_rewards);

        let expected_reward_per_subscriber = total_mapper_rewards / NUM_SUBSCRIBERS;

        // get the summed rewards allocated to subscribers for discovery location
        let mut allocated_mapper_rewards = 0_u64;
        for (reward_amount, subscriber_share) in
            mapping_shares.into_subscriber_rewards(&epoch, rewards_per_share)
        {
            if let Some(MobileReward::SubscriberReward(r)) = subscriber_share.reward {
                assert_eq!(expected_reward_per_subscriber, r.discovery_location_amount);
                assert_eq!(reward_amount, r.discovery_location_amount);
                allocated_mapper_rewards += reward_amount;
            }
        }

        // verify the total rewards awarded for discovery mapping
        assert_eq!(16_393_442_620_000, allocated_mapper_rewards);

        // confirm the unallocated service provider reward amounts
        // this should not be more than the total number of subscribers ( 10 k)
        // as we can at max drop one bone per subscriber due to rounding
        let unallocated_mapper_reward_amount = total_mapper_rewards - allocated_mapper_rewards;
        assert_eq!(unallocated_mapper_reward_amount, 2950);
        assert!(unallocated_mapper_reward_amount < NUM_SUBSCRIBERS);
    }

    /// Test to ensure that the correct data transfer amount is rewarded.
    #[tokio::test]
    async fn ensure_data_correct_transfer_reward_amount() {
        let owner: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner parse");
        let payer: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed payer parse");

        let data_transfer_session = HotspotDataSession {
            pub_key: owner.clone(),
            payer,
            upload_bytes: 0,   // Unused
            download_bytes: 0, // Unused
            num_dcs: 2,
            received_timestamp: DateTime::default(),
        };

        let mut data_transfer_map = HotspotMap::new();
        data_transfer_map.insert(
            data_transfer_session.pub_key,
            HotspotReward {
                rewardable_bytes: 0, // Not used
                rewardable_dc: data_transfer_session.num_dcs as u64,
            },
        );

        let now = Utc::now();
        let epoch = (now - Duration::hours(1))..now;
        let total_rewards = get_scheduled_tokens_for_poc(epoch.end - epoch.start);

        // confirm our hourly rewards add up to expected 24hr amount
        // total_rewards will be in bones
        assert_eq!(
            (total_rewards / dec!(1_000_000) * dec!(24)).trunc(),
            dec!(49_180_327)
        );

        let data_transfer_rewards =
            TransferRewards::from_transfer_sessions(dec!(1.0), data_transfer_map, &epoch).await;

        assert_eq!(data_transfer_rewards.reward(&owner), dec!(0.00002));
        assert_eq!(data_transfer_rewards.reward_scale(), dec!(1.0));
        let available_poc_rewards = get_scheduled_tokens_for_poc(epoch.end - epoch.start)
            - data_transfer_rewards.reward_sum;
        assert_eq!(
            available_poc_rewards,
            total_rewards
                - (data_transfer_rewards.reward(&owner) * data_transfer_rewards.reward_scale())
        );
    }

    /// Test to ensure that excess transfer rewards are properly scaled down.
    #[tokio::test]
    async fn ensure_excess_transfer_rewards_scale() {
        let owner: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner parse");
        let payer: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed payer parse");

        let mut transfer_sessions = Vec::new();
        // Just an absurdly large amount of DC
        for _ in 0..3_003 {
            transfer_sessions.push(Ok(HotspotDataSession {
                pub_key: owner.clone(),
                payer: payer.clone(),
                upload_bytes: 0,
                download_bytes: 0,
                num_dcs: 2222222222222222,
                received_timestamp: DateTime::default(),
            }));
        }
        let data_transfer_sessions = stream::iter(transfer_sessions);
        let aggregated_data_transfer_sessions =
            data_session::data_sessions_to_dc(data_transfer_sessions)
                .await
                .unwrap();

        let now = Utc::now();
        let epoch = (now - Duration::hours(24))..now;

        let data_transfer_rewards = TransferRewards::from_transfer_sessions(
            dec!(1.0),
            aggregated_data_transfer_sessions,
            &epoch,
        )
        .await;

        // We have constructed the data transfer in such a way that they easily exceed the maximum
        // allotted reward amount for data transfer, which is 40% of the daily tokens. We check to
        // ensure that amount of tokens remaining for POC is no less than 20% of the rewards allocated
        // for POC and data transfer (which is 60% of the daily total emissions).
        let available_poc_rewards = get_scheduled_tokens_for_poc(epoch.end - epoch.start)
            - data_transfer_rewards.reward_sum;
        assert_eq!(available_poc_rewards.trunc(), dec!(16_393_442_622_950));
        assert_eq!(
            // Rewards are automatically scaled
            data_transfer_rewards.reward(&owner).trunc(),
            dec!(32_786_885_245_901)
        );
        assert_eq!(data_transfer_rewards.reward_scale().round_dp(1), dec!(0.5));
    }

    fn bytes_per_s(mbps: u64) -> u64 {
        mbps * 125000
    }

    fn acceptable_speedtest(pubkey: PublicKeyBinary, timestamp: DateTime<Utc>) -> Speedtest {
        Speedtest {
            report: CellSpeedtest {
                pubkey,
                timestamp,
                upload_speed: bytes_per_s(10),
                download_speed: bytes_per_s(100),
                latency: 25,
                serial: "".to_string(),
            },
        }
    }

    fn degraded_speedtest(pubkey: PublicKeyBinary, timestamp: DateTime<Utc>) -> Speedtest {
        Speedtest {
            report: CellSpeedtest {
                pubkey,
                timestamp,
                upload_speed: bytes_per_s(5),
                download_speed: bytes_per_s(60),
                latency: 60,
                serial: "".to_string(),
            },
        }
    }

    fn failed_speedtest(pubkey: PublicKeyBinary, timestamp: DateTime<Utc>) -> Speedtest {
        Speedtest {
            report: CellSpeedtest {
                pubkey,
                timestamp,
                upload_speed: bytes_per_s(1),
                download_speed: bytes_per_s(20),
                latency: 110,
                serial: "".to_string(),
            },
        }
    }

    fn poor_speedtest(pubkey: PublicKeyBinary, timestamp: DateTime<Utc>) -> Speedtest {
        Speedtest {
            report: CellSpeedtest {
                pubkey,
                timestamp,
                upload_speed: bytes_per_s(2),
                download_speed: bytes_per_s(40),
                latency: 90,
                serial: "".to_string(),
            },
        }
    }

    #[async_trait::async_trait]
    impl CoveredHexStream for HashMap<(OwnedKeyType, Uuid), Vec<HexCoverage>> {
        async fn covered_hex_stream<'a>(
            &'a self,
            key: KeyType<'a>,
            coverage_obj: &'a Uuid,
            _seniority: &'a Seniority,
        ) -> Result<BoxStream<'a, Result<HexCoverage, sqlx::Error>>, sqlx::Error> {
            Ok(
                stream::iter(self.get(&(key.to_owned(), *coverage_obj)).unwrap().clone())
                    .map(Ok)
                    .boxed(),
            )
        }
        async fn fetch_seniority(
            &self,
            _key: KeyType<'_>,
            _period_end: DateTime<Utc>,
        ) -> Result<Seniority, sqlx::Error> {
            Ok(Seniority {
                uuid: Uuid::new_v4(),
                seniority_ts: DateTime::default(),
                last_heartbeat: DateTime::default(),
                inserted_at: DateTime::default(),
                update_reason: 0,
            })
        }
    }

    fn simple_hex_coverage<'a>(key: impl Into<KeyType<'a>>, hex: u64) -> Vec<HexCoverage> {
        let key = key.into();
        let radio_key = key.to_owned();
        vec![HexCoverage {
            uuid: Uuid::new_v4(),
            hex: hex as i64,
            indoor: true,
            radio_key,
            signal_level: crate::coverage::SignalLevel::Low,
            signal_power: 0,
            coverage_claim_time: DateTime::<Utc>::MIN_UTC,
            inserted_at: DateTime::<Utc>::MIN_UTC,
        }]
    }

    /// Test to ensure that different speedtest averages correctly afferct reward shares.
    #[tokio::test]
    async fn ensure_speedtest_averages_affect_reward_shares() {
        // init owners
        let owner1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner1 parse");
        let owner2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed owner2 parse");
        let owner3: PublicKeyBinary = "112DJZiXvZ8FduiWrEi8siE3wJX6hpRjjtwbavyXUDkgutEUSLAE"
            .parse()
            .expect("failed owner3 parse");
        let owner4: PublicKeyBinary = "112p1GbUtRLyfFaJr1XF8fH7yz9cSZ4exbrSpVDeu67DeGb31QUL"
            .parse()
            .expect("failed owner4 parse");
        let owner5: PublicKeyBinary = "112bUGwooPd1dCDd3h3yZwskjxCzBsQNKeaJTuUF4hSgYedcsFa9"
            .parse()
            .expect("failed owner5 parse");
        let owner6: PublicKeyBinary = "112WqD16uH8GLmCMhyRUrp6Rw5MTELzBdx7pSepySYUoSjixQoxJ"
            .parse()
            .expect("failed owner6 parse");
        let owner7: PublicKeyBinary = "112WnYhq4qX3wdw6JTZT3w3A9FNGxeescJwJffcBN5jiZvovWRkQ"
            .parse()
            .expect("failed owner7 parse");

        // init hotspots
        let gw1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed gw1 parse");
        let gw2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed gw2 parse");
        let gw3: PublicKeyBinary = "112DJZiXvZ8FduiWrEi8siE3wJX6hpRjjtwbavyXUDkgutEUSLAE"
            .parse()
            .expect("failed gw3 parse");
        let gw4: PublicKeyBinary = "112p1GbUtRLyfFaJr1XF8fH7yz9cSZ4exbrSpVDeu67DeGb31QUL"
            .parse()
            .expect("failed gw4 parse");
        let gw5: PublicKeyBinary = "112j1iw1sV2B2Tz2DxPSeum9Cmc5kMKNdDTDg1zDRsdwuvZueq3B"
            .parse()
            .expect("failed gw5 parse");
        let gw6: PublicKeyBinary = "11fCasUk9XvU15ktsMMH64J9E7XuqQ2L5FJPv8HZMCDG6kdZ3SC"
            .parse()
            .expect("failed gw6 parse");
        let gw7: PublicKeyBinary = "11HdwRpQDrYM7LJtRGSzRF3vY2iwuumx1Z2MUhBYAVTwZdSh6Bi"
            .parse()
            .expect("failed gw7 parse");
        let gw8: PublicKeyBinary = "112qDCKek7fePg6wTpEnbLp3uD7TTn8MBH7PGKtmAaUcG1vKQ9eZ"
            .parse()
            .expect("failed gw8 parse");
        // include a couple of wifi spots in the mix
        let gw9: PublicKeyBinary = "112bUuQaE7j73THS9ABShHGokm46Miip9L361FSyWv7zSYn8hZWf"
            .parse()
            .expect("failed gw9 parse");
        let gw10: PublicKeyBinary = "11z69eJ3czc92k6snrfR9ek7g2uRWXosFbnG9v4bXgwhfUCivUo"
            .parse()
            .expect("failed gw10 parse");
        let gw11: PublicKeyBinary = "112WnYhq4qX3wdw6JTZT3w3A9FNGxeescJwJffcBN5jiZvovWRkQ"
            .parse()
            .expect("failed gw11 parse");

        // link gws to owners
        let mut owners = HashMap::new();
        owners.insert(gw1.clone(), owner1.clone());
        owners.insert(gw2.clone(), owner1.clone());
        owners.insert(gw3.clone(), owner1.clone());
        owners.insert(gw4.clone(), owner2.clone());
        owners.insert(gw5.clone(), owner2.clone());
        owners.insert(gw6.clone(), owner3.clone());
        owners.insert(gw7.clone(), owner3.clone());
        owners.insert(gw8.clone(), owner4.clone());
        owners.insert(gw9.clone(), owner5.clone());
        owners.insert(gw10.clone(), owner6.clone());
        owners.insert(gw11.clone(), owner7.clone());

        // init cells and cell_types
        let c2 = "P27-SCE4255W2107CW5000015".to_string();
        let c4 = "2AG32PBS3101S1202000464223GY0154".to_string();
        let c5 = "P27-SCE4255W2107CW5000016".to_string();
        let c6 = "2AG32PBS3101S1202000464223GY0155".to_string();
        let c7 = "2AG32PBS3101S1202000464223GY0156".to_string();
        let c8 = "P27-SCE4255W2107CW5000017".to_string();
        let c9 = "P27-SCE4255W2107CW5000018".to_string();
        let c10 = "P27-SCE4255W2107CW5000019".to_string();
        let c11 = "P27-SCE4255W2107CW5000020".to_string();
        let c12 = "P27-SCE4255W2107CW5000021".to_string();
        let c13 = "P27-SCE4255W2107CW5000022".to_string();
        let c14 = "2AG32PBS3101S1202000464223GY0157".to_string();

        let cov_obj_2 = Uuid::new_v4();
        let cov_obj_4 = Uuid::new_v4();
        let cov_obj_5 = Uuid::new_v4();
        let cov_obj_6 = Uuid::new_v4();
        let cov_obj_7 = Uuid::new_v4();
        let cov_obj_8 = Uuid::new_v4();
        let cov_obj_9 = Uuid::new_v4();
        let cov_obj_10 = Uuid::new_v4();
        let cov_obj_11 = Uuid::new_v4();
        let cov_obj_12 = Uuid::new_v4();
        let cov_obj_13 = Uuid::new_v4();
        let cov_obj_14 = Uuid::new_v4();
        let cov_obj_15 = Uuid::new_v4();
        let cov_obj_16 = Uuid::new_v4();
        let cov_obj_17 = Uuid::new_v4();

        let now = Utc::now();
        let timestamp = now - Duration::minutes(20);

        // setup heartbeats
        let heartbeat_rewards = vec![
            HeartbeatReward {
                cbsd_id: Some(c2.clone()),
                hotspot_key: gw2.clone(),
                coverage_object: cov_obj_2,
                cell_type: CellType::from_cbsd_id(&c2).unwrap(),
                distances_to_asserted: None,
                trust_score_multipliers: vec![dec!(1.0)],
            },
            HeartbeatReward {
                cbsd_id: Some(c4.clone()),
                hotspot_key: gw3.clone(),
                coverage_object: cov_obj_4,
                cell_type: CellType::from_cbsd_id(&c4).unwrap(),
                distances_to_asserted: None,
                trust_score_multipliers: vec![dec!(1.0)],
            },
            HeartbeatReward {
                cbsd_id: Some(c5.clone()),
                hotspot_key: gw4.clone(),
                coverage_object: cov_obj_5,
                cell_type: CellType::from_cbsd_id(&c5).unwrap(),
                distances_to_asserted: None,
                trust_score_multipliers: vec![dec!(1.0)],
            },
            HeartbeatReward {
                cbsd_id: Some(c6.clone()),
                hotspot_key: gw4.clone(),
                coverage_object: cov_obj_6,
                cell_type: CellType::from_cbsd_id(&c6).unwrap(),
                distances_to_asserted: None,
                trust_score_multipliers: vec![dec!(1.0)],
            },
            HeartbeatReward {
                cbsd_id: Some(c7.clone()),
                hotspot_key: gw4.clone(),
                coverage_object: cov_obj_7,
                cell_type: CellType::from_cbsd_id(&c7).unwrap(),
                distances_to_asserted: None,
                trust_score_multipliers: vec![dec!(1.0)],
            },
            HeartbeatReward {
                cbsd_id: Some(c8.clone()),
                hotspot_key: gw4.clone(),
                coverage_object: cov_obj_8,
                cell_type: CellType::from_cbsd_id(&c8).unwrap(),
                distances_to_asserted: None,
                trust_score_multipliers: vec![dec!(1.0)],
            },
            HeartbeatReward {
                cbsd_id: Some(c9.clone()),
                hotspot_key: gw4.clone(),
                coverage_object: cov_obj_9,
                cell_type: CellType::from_cbsd_id(&c9).unwrap(),
                distances_to_asserted: None,
                trust_score_multipliers: vec![dec!(1.0)],
            },
            HeartbeatReward {
                cbsd_id: Some(c10.clone()),
                hotspot_key: gw4.clone(),
                coverage_object: cov_obj_10,
                cell_type: CellType::from_cbsd_id(&c10).unwrap(),
                distances_to_asserted: None,
                trust_score_multipliers: vec![dec!(1.0)],
            },
            HeartbeatReward {
                cbsd_id: Some(c11.clone()),
                hotspot_key: gw4.clone(),
                coverage_object: cov_obj_11,
                cell_type: CellType::from_cbsd_id(&c11).unwrap(),
                distances_to_asserted: None,
                trust_score_multipliers: vec![dec!(1.0)],
            },
            HeartbeatReward {
                cbsd_id: Some(c12.clone()),
                hotspot_key: gw5.clone(),
                coverage_object: cov_obj_12,
                cell_type: CellType::from_cbsd_id(&c12).unwrap(),
                distances_to_asserted: None,
                trust_score_multipliers: vec![dec!(1.0)],
            },
            HeartbeatReward {
                cbsd_id: Some(c13.clone()),
                hotspot_key: gw6.clone(),
                coverage_object: cov_obj_13,
                cell_type: CellType::from_cbsd_id(&c13).unwrap(),
                distances_to_asserted: None,
                trust_score_multipliers: vec![dec!(1.0)],
            },
            HeartbeatReward {
                cbsd_id: Some(c14.clone()),
                hotspot_key: gw7.clone(),
                coverage_object: cov_obj_14,
                cell_type: CellType::from_cbsd_id(&c14).unwrap(),
                distances_to_asserted: None,
                trust_score_multipliers: vec![dec!(1.0)],
            },
            HeartbeatReward {
                cbsd_id: None,
                hotspot_key: gw9.clone(),
                cell_type: CellType::NovaGenericWifiIndoor,
                coverage_object: cov_obj_15,
                distances_to_asserted: Some(vec![0]),
                trust_score_multipliers: vec![dec!(1.0)],
            },
            HeartbeatReward {
                cbsd_id: None,
                hotspot_key: gw10.clone(),
                cell_type: CellType::NovaGenericWifiIndoor,
                coverage_object: cov_obj_16,
                distances_to_asserted: Some(vec![0]),
                trust_score_multipliers: vec![dec!(0.25)],
            },
            HeartbeatReward {
                cbsd_id: None,
                hotspot_key: gw11.clone(),
                cell_type: CellType::NovaGenericWifiIndoor,
                coverage_object: cov_obj_17,
                distances_to_asserted: Some(vec![0]),
                trust_score_multipliers: vec![dec!(0.25)],
            },
        ]
        .into_iter()
        .map(Ok)
        .collect::<Vec<Result<HeartbeatReward, _>>>();

        // Setup hex coverages
        let mut hex_coverage = HashMap::new();
        hex_coverage.insert(
            (OwnedKeyType::from(c2.clone()), cov_obj_2),
            simple_hex_coverage(&c2, 0x8a1fb46622dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c4.clone()), cov_obj_4),
            simple_hex_coverage(&c4, 0x8a1fb46632dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c5.clone()), cov_obj_5),
            simple_hex_coverage(&c5, 0x8a1fb46642dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c6.clone()), cov_obj_6),
            simple_hex_coverage(&c6, 0x8a1fb46652dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c7.clone()), cov_obj_7),
            simple_hex_coverage(&c7, 0x8a1fb46662dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c8.clone()), cov_obj_8),
            simple_hex_coverage(&c8, 0x8a1fb46522dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c9.clone()), cov_obj_9),
            simple_hex_coverage(&c9, 0x8a1fb46682dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c10.clone()), cov_obj_10),
            simple_hex_coverage(&c10, 0x8a1fb46692dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c11.clone()), cov_obj_11),
            simple_hex_coverage(&c11, 0x8a1fb466a2dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c12.clone()), cov_obj_12),
            simple_hex_coverage(&c12, 0x8a1fb466b2dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c13.clone()), cov_obj_13),
            simple_hex_coverage(&c13, 0x8a1fb466c2dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c14.clone()), cov_obj_14),
            simple_hex_coverage(&c14, 0x8a1fb466d2dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(gw9.clone()), cov_obj_15),
            simple_hex_coverage(&gw9, 0x8c2681a30641dff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(gw10.clone()), cov_obj_16),
            simple_hex_coverage(&gw10, 0x8c2681a3065d3ff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(gw11.clone()), cov_obj_17),
            simple_hex_coverage(&gw11, 0x8c2681a306607ff),
        );

        // setup speedtests
        let last_speedtest = timestamp - Duration::hours(12);
        let gw1_speedtests = vec![
            acceptable_speedtest(gw1.clone(), last_speedtest),
            acceptable_speedtest(gw1.clone(), timestamp),
        ];
        let gw2_speedtests = vec![
            acceptable_speedtest(gw2.clone(), last_speedtest),
            acceptable_speedtest(gw2.clone(), timestamp),
        ];
        let gw3_speedtests = vec![
            acceptable_speedtest(gw3.clone(), last_speedtest),
            acceptable_speedtest(gw3.clone(), timestamp),
        ];
        let gw4_speedtests = vec![
            acceptable_speedtest(gw4.clone(), last_speedtest),
            acceptable_speedtest(gw4.clone(), timestamp),
        ];
        let gw5_speedtests = vec![
            degraded_speedtest(gw5.clone(), last_speedtest),
            degraded_speedtest(gw5.clone(), timestamp),
        ];
        let gw6_speedtests = vec![
            failed_speedtest(gw6.clone(), last_speedtest),
            failed_speedtest(gw6.clone(), timestamp),
        ];
        let gw7_speedtests = vec![
            poor_speedtest(gw7.clone(), last_speedtest),
            poor_speedtest(gw7.clone(), timestamp),
        ];
        let gw9_speedtests = vec![
            acceptable_speedtest(gw9.clone(), last_speedtest),
            acceptable_speedtest(gw9.clone(), timestamp),
        ];
        let gw10_speedtests = vec![
            acceptable_speedtest(gw10.clone(), last_speedtest),
            acceptable_speedtest(gw10.clone(), timestamp),
        ];
        let gw11_speedtests = vec![
            acceptable_speedtest(gw11.clone(), last_speedtest),
            acceptable_speedtest(gw11.clone(), timestamp),
        ];

        let gw1_average = SpeedtestAverage::from(gw1_speedtests);
        let gw2_average = SpeedtestAverage::from(gw2_speedtests);
        let gw3_average = SpeedtestAverage::from(gw3_speedtests);
        let gw4_average = SpeedtestAverage::from(gw4_speedtests);
        let gw5_average = SpeedtestAverage::from(gw5_speedtests);
        let gw6_average = SpeedtestAverage::from(gw6_speedtests);
        let gw7_average = SpeedtestAverage::from(gw7_speedtests);
        let gw9_average = SpeedtestAverage::from(gw9_speedtests);
        let gw10_average = SpeedtestAverage::from(gw10_speedtests);
        let gw11_average = SpeedtestAverage::from(gw11_speedtests);
        let mut averages = HashMap::new();
        averages.insert(gw1.clone(), gw1_average);
        averages.insert(gw2.clone(), gw2_average);
        averages.insert(gw3.clone(), gw3_average);
        averages.insert(gw4.clone(), gw4_average);
        averages.insert(gw5.clone(), gw5_average);
        averages.insert(gw6.clone(), gw6_average);
        averages.insert(gw7.clone(), gw7_average);
        averages.insert(gw9.clone(), gw9_average);
        averages.insert(gw10.clone(), gw10_average);
        averages.insert(gw11.clone(), gw11_average);

        let speedtest_avgs = SpeedtestAverages { averages };

        // calculate the rewards for the sample group
        let mut owner_rewards = HashMap::<PublicKeyBinary, u64>::new();

        let duration = Duration::hours(1);
        let epoch = (now - duration)..now;
        let total_poc_rewards = get_scheduled_tokens_for_poc(epoch.end - epoch.start);
        let mut allocated_poc_rewards = 0_u64;

        let epoch = (now - Duration::hours(1))..now;
        for (reward_amount, mobile_reward) in CoveragePoints::aggregate_points(
            &hex_coverage,
            stream::iter(heartbeat_rewards),
            &speedtest_avgs,
            &BoostedHexes::default(),
            &epoch,
        )
        .await
        .unwrap()
        .into_rewards(total_poc_rewards, &epoch)
        .unwrap()
        {
            let radio_reward = match mobile_reward.reward {
                Some(proto::mobile_reward_share::Reward::RadioReward(radio_reward)) => radio_reward,
                _ => unreachable!(),
            };
            let owner = owners
                .get(&PublicKeyBinary::from(radio_reward.hotspot_key))
                .expect("Could not find owner")
                .clone();
            assert_eq!(reward_amount, radio_reward.poc_reward);
            allocated_poc_rewards += reward_amount;
            *owner_rewards.entry(owner).or_default() += radio_reward.poc_reward;
        }

        assert_eq!(
            *owner_rewards
                .get(&owner1)
                .expect("Could not fetch owner1 rewards"),
            364_298_724_954
        );
        assert_eq!(
            *owner_rewards
                .get(&owner2)
                .expect("Could not fetch owner2 rewards"),
            1_366_120_218_577
        );
        assert_eq!(
            *owner_rewards
                .get(&owner3)
                .expect("Could not fetch owner3 rewards"),
            45_537_340_619
        );
        assert_eq!(owner_rewards.get(&owner4), None);

        let owner5_reward = *owner_rewards
            .get(&owner5)
            .expect("Could not fetch owner5 rewards");
        assert_eq!(owner5_reward, 182_149_362_477);

        let owner6_reward = *owner_rewards
            .get(&owner6)
            .expect("Could not fetch owner6 rewards");
        assert_eq!(owner6_reward, 45_537_340_619);

        // confirm owner 6 reward is 0.25 of owner 5's reward
        // this is due to owner 6's hotspot not having a validation location timestamp
        // and thus its reward scale is reduced
        assert_eq!((owner5_reward as f64 * 0.25) as u64, owner6_reward);

        let owner7_reward = *owner_rewards
            .get(&owner6)
            .expect("Could not fetch owner7 rewards");
        assert_eq!(owner7_reward, 45_537_340_619);

        // confirm owner 7 reward is 0.25 of owner 5's reward
        // owner 7's hotspot does have a validation location timestamp
        // but its distance beyond the asserted location is too high
        // and thus its reward scale is reduced
        assert_eq!((owner5_reward as f64 * 0.25) as u64, owner7_reward);

        // confirm total sum of allocated poc rewards
        assert_eq!(allocated_poc_rewards, 2_049_180_327_865);

        // confirm the unallocated poc reward amounts
        let unallocated_sp_reward_amount = (total_poc_rewards
            - Decimal::from(allocated_poc_rewards))
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0);
        assert_eq!(unallocated_sp_reward_amount, 3);
    }

    #[tokio::test]
    async fn full_wifi_indoor_vs_sercomm_indoor_reward_shares() {
        // init owners
        let owner1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner1 parse");
        let owner2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed owner2 parse");
        // init hotspots
        let gw1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed gw1 parse");
        let gw2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed gw2 parse");
        // link gws to owners
        let mut owners = HashMap::new();
        owners.insert(gw1.clone(), owner1.clone());
        owners.insert(gw2.clone(), owner2.clone());

        let now = Utc::now();
        let timestamp = now - Duration::minutes(20);

        let g1_cov_obj = Uuid::new_v4();
        let g2_cov_obj = Uuid::new_v4();

        // init cells and cell_types
        let c2 = "P27-SCE4255W".to_string(); // sercom indoor

        // setup heartbeats
        let heartbeat_rewards = vec![
            // add wifi indoor HB
            HeartbeatReward {
                cbsd_id: None,
                hotspot_key: gw1.clone(),
                cell_type: CellType::NovaGenericWifiIndoor,
                coverage_object: g1_cov_obj,
                distances_to_asserted: Some(vec![0]),
                trust_score_multipliers: vec![dec!(1.0)],
            },
            // add sercomm indoor HB
            HeartbeatReward {
                cbsd_id: Some(c2.clone()),
                hotspot_key: gw2.clone(),
                cell_type: CellType::from_cbsd_id(&c2).unwrap(),
                coverage_object: g2_cov_obj,
                distances_to_asserted: None,
                trust_score_multipliers: vec![dec!(1.0)],
            },
        ]
        .into_iter()
        .map(Ok)
        .collect::<Vec<Result<HeartbeatReward, _>>>();

        // setup speedtests
        let last_speedtest = timestamp - Duration::hours(12);
        let gw1_speedtests = vec![
            acceptable_speedtest(gw1.clone(), last_speedtest),
            acceptable_speedtest(gw1.clone(), timestamp),
        ];
        let gw2_speedtests = vec![
            acceptable_speedtest(gw2.clone(), last_speedtest),
            acceptable_speedtest(gw2.clone(), timestamp),
        ];

        let gw1_average = SpeedtestAverage::from(gw1_speedtests);
        let gw2_average = SpeedtestAverage::from(gw2_speedtests);
        let mut averages = HashMap::new();
        averages.insert(gw1.clone(), gw1_average);
        averages.insert(gw2.clone(), gw2_average);

        let speedtest_avgs = SpeedtestAverages { averages };
        let mut hex_coverage: HashMap<(OwnedKeyType, Uuid), Vec<HexCoverage>> = Default::default();
        hex_coverage.insert(
            (OwnedKeyType::from(gw1.clone()), g1_cov_obj),
            simple_hex_coverage(&gw1, 0x8a1fb46622dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c2.clone()), g2_cov_obj),
            simple_hex_coverage(&c2, 0x8a1fb46642dffff),
        );

        // calculate the rewards for the group
        let mut owner_rewards = HashMap::<PublicKeyBinary, u64>::new();
        let duration = Duration::hours(1);
        let epoch = (now - duration)..now;
        let total_poc_rewards = get_scheduled_tokens_for_poc(epoch.end - epoch.start);
        for (_reward_amount, mobile_reward) in CoveragePoints::aggregate_points(
            &hex_coverage,
            stream::iter(heartbeat_rewards),
            &speedtest_avgs,
            &BoostedHexes::default(),
            &epoch,
        )
        .await
        .unwrap()
        .into_rewards(total_poc_rewards, &epoch)
        .unwrap()
        {
            let radio_reward = match mobile_reward.reward {
                Some(proto::mobile_reward_share::Reward::RadioReward(radio_reward)) => radio_reward,
                _ => unreachable!(),
            };
            let owner = owners
                .get(&PublicKeyBinary::from(radio_reward.hotspot_key))
                .expect("Could not find owner")
                .clone();

            *owner_rewards.entry(owner).or_default() += radio_reward.poc_reward;
        }
        println!("owner rewards {:?}", owner_rewards);

        // These were different, now they are the same:

        // wifi
        let owner1_reward = *owner_rewards
            .get(&owner1)
            .expect("Could not fetch owner1 rewards");
        assert_eq!(owner1_reward, 1_024_590_163_934);

        // sercomm
        let owner2_reward = *owner_rewards
            .get(&owner2)
            .expect("Could not fetch owner2 rewards");
        assert_eq!(owner2_reward, 1_024_590_163_934);
    }

    #[tokio::test]
    async fn reduced_wifi_indoor_vs_sercomm_indoor_reward_shares() {
        // init owners
        let owner1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner1 parse");
        let owner2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed owner2 parse");
        // init hotspots
        let gw1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed gw1 parse");
        let gw2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed gw2 parse");
        // link gws to owners
        let mut owners = HashMap::new();
        owners.insert(gw1.clone(), owner1.clone());
        owners.insert(gw2.clone(), owner2.clone());

        let now = Utc::now();
        let timestamp = now - Duration::minutes(20);

        // init cells and cell_types
        let c2 = "P27-SCE4255W".to_string(); // sercom indoor

        let g1_cov_obj = Uuid::new_v4();
        let g2_cov_obj = Uuid::new_v4();

        // setup heartbeats
        let heartbeat_rewards = vec![
            // add wifi  indoor HB
            // with distance to asserted > than max allowed
            // this results in reward scale dropping to 0.25
            HeartbeatReward {
                cbsd_id: None,
                hotspot_key: gw1.clone(),
                cell_type: CellType::NovaGenericWifiIndoor,
                coverage_object: g1_cov_obj,
                distances_to_asserted: Some(vec![0]),
                trust_score_multipliers: vec![dec!(0.25)],
            },
            // add sercomm indoor HB
            HeartbeatReward {
                cbsd_id: Some(c2.clone()),
                hotspot_key: gw2.clone(),
                coverage_object: g2_cov_obj,
                cell_type: CellType::from_cbsd_id(&c2).unwrap(),
                distances_to_asserted: None,
                trust_score_multipliers: vec![dec!(1.0)],
            },
        ]
        .into_iter()
        .map(Ok)
        .collect::<Vec<Result<HeartbeatReward, _>>>();

        // setup speedtests
        let last_speedtest = timestamp - Duration::hours(12);
        let gw1_speedtests = vec![
            acceptable_speedtest(gw1.clone(), last_speedtest),
            acceptable_speedtest(gw1.clone(), timestamp),
        ];
        let gw2_speedtests = vec![
            acceptable_speedtest(gw2.clone(), last_speedtest),
            acceptable_speedtest(gw2.clone(), timestamp),
        ];

        let gw1_average = SpeedtestAverage::from(gw1_speedtests);
        let gw2_average = SpeedtestAverage::from(gw2_speedtests);
        let mut averages = HashMap::new();
        averages.insert(gw1.clone(), gw1_average);
        averages.insert(gw2.clone(), gw2_average);

        let speedtest_avgs = SpeedtestAverages { averages };

        let mut hex_coverage: HashMap<(OwnedKeyType, Uuid), Vec<HexCoverage>> = Default::default();
        hex_coverage.insert(
            (OwnedKeyType::from(gw1.clone()), g1_cov_obj),
            simple_hex_coverage(&gw1, 0x8a1fb46622dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c2.clone()), g2_cov_obj),
            simple_hex_coverage(&c2, 0x8a1fb46642dffff),
        );

        // calculate the rewards for the group
        let mut owner_rewards = HashMap::<PublicKeyBinary, u64>::new();
        let duration = Duration::hours(1);
        let epoch = (now - duration)..now;
        let total_poc_rewards = get_scheduled_tokens_for_poc(epoch.end - epoch.start);
        for (_reward_amount, mobile_reward) in CoveragePoints::aggregate_points(
            &hex_coverage,
            stream::iter(heartbeat_rewards),
            &speedtest_avgs,
            &BoostedHexes::default(),
            &epoch,
        )
        .await
        .unwrap()
        .into_rewards(total_poc_rewards, &epoch)
        .unwrap()
        {
            let radio_reward = match mobile_reward.reward {
                Some(proto::mobile_reward_share::Reward::RadioReward(radio_reward)) => radio_reward,
                _ => unreachable!(),
            };
            let owner = owners
                .get(&PublicKeyBinary::from(radio_reward.hotspot_key))
                .expect("Could not find owner")
                .clone();

            *owner_rewards.entry(owner).or_default() += radio_reward.poc_reward;
        }

        // wifi
        let owner1_reward = *owner_rewards
            .get(&owner1)
            .expect("Could not fetch owner1 rewards");

        // sercomm
        let owner2_reward = *owner_rewards
            .get(&owner2)
            .expect("Could not fetch owner2 rewards");

        // confirm owner 1 reward is 0.1 of owner 2's reward
        // owner 1 is a wifi indoor with a distance_to_asserted > max
        // and so gets the reduced reward scale of 0.1 ( radio reward scale of 0.4 * location scale of 0.25)
        // owner 2 is a cbrs sercomm indoor which has a reward scale of 1.0
        assert_eq!(owner1_reward, (owner2_reward as f64 * 0.25) as u64);
    }

    #[tokio::test]
    async fn full_wifi_outdoor_vs_sercomm_indoor_reward_shares() {
        // init owners
        let owner1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner1 parse");
        let owner2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed owner2 parse");
        // init hotspots
        let gw1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed gw1 parse");
        let gw2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed gw2 parse");
        // link gws to owners
        let mut owners = HashMap::new();
        owners.insert(gw1.clone(), owner1.clone());
        owners.insert(gw2.clone(), owner2.clone());

        let now = Utc::now();
        let timestamp = now - Duration::minutes(20);

        let g1_cov_obj = Uuid::new_v4();
        let g2_cov_obj = Uuid::new_v4();

        // init cells and cell_types
        let c2 = "P27-SCE4255W".to_string(); // sercom indoor

        // setup heartbeats
        let heartbeat_rewards = vec![
            // add wifi indoor HB
            HeartbeatReward {
                cbsd_id: None,
                hotspot_key: gw1.clone(),
                cell_type: CellType::NovaGenericWifiOutdoor,
                coverage_object: g1_cov_obj,
                distances_to_asserted: Some(vec![0]),
                trust_score_multipliers: vec![dec!(1.0)],
            },
            // add sercomm indoor HB
            HeartbeatReward {
                cbsd_id: Some(c2.clone()),
                hotspot_key: gw2.clone(),
                cell_type: CellType::from_cbsd_id(&c2).unwrap(),
                coverage_object: g2_cov_obj,
                distances_to_asserted: None,
                trust_score_multipliers: vec![dec!(1.0)],
            },
        ]
        .into_iter()
        .map(Ok)
        .collect::<Vec<Result<HeartbeatReward, _>>>();

        // setup speedtests
        let last_speedtest = timestamp - Duration::hours(12);
        let gw1_speedtests = vec![
            acceptable_speedtest(gw1.clone(), last_speedtest),
            acceptable_speedtest(gw1.clone(), timestamp),
        ];
        let gw2_speedtests = vec![
            acceptable_speedtest(gw2.clone(), last_speedtest),
            acceptable_speedtest(gw2.clone(), timestamp),
        ];

        let gw1_average = SpeedtestAverage::from(gw1_speedtests);
        let gw2_average = SpeedtestAverage::from(gw2_speedtests);
        let mut averages = HashMap::new();
        averages.insert(gw1.clone(), gw1_average);
        averages.insert(gw2.clone(), gw2_average);

        let speedtest_avgs = SpeedtestAverages { averages };
        let mut hex_coverage: HashMap<(OwnedKeyType, Uuid), Vec<HexCoverage>> = Default::default();
        hex_coverage.insert(
            (OwnedKeyType::from(gw1.clone()), g1_cov_obj),
            simple_hex_coverage(&gw1, 0x8a1fb46622dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c2.clone()), g2_cov_obj),
            simple_hex_coverage(&c2, 0x8a1fb46642dffff),
        );

        // calculate the rewards for the group
        let mut owner_rewards = HashMap::<PublicKeyBinary, u64>::new();
        let duration = Duration::hours(1);
        let epoch = (now - duration)..now;
        let total_poc_rewards = get_scheduled_tokens_for_poc(epoch.end - epoch.start);
        for (_reward_amount, mobile_reward) in CoveragePoints::aggregate_points(
            &hex_coverage,
            stream::iter(heartbeat_rewards),
            &speedtest_avgs,
            &BoostedHexes::default(),
            &epoch,
        )
        .await
        .unwrap()
        .into_rewards(total_poc_rewards, &epoch)
        .unwrap()
        {
            let radio_reward = match mobile_reward.reward {
                Some(proto::mobile_reward_share::Reward::RadioReward(radio_reward)) => radio_reward,
                _ => unreachable!(),
            };
            let owner = owners
                .get(&PublicKeyBinary::from(radio_reward.hotspot_key))
                .expect("Could not find owner")
                .clone();

            *owner_rewards.entry(owner).or_default() += radio_reward.poc_reward;
        }

        // These were different, now they are the same:
        println!("owner rewards {:?}", owner_rewards);
        // wifi
        let owner1_reward = *owner_rewards
            .get(&owner1)
            .expect("Could not fetch owner1 rewards");
        assert_eq!(owner1_reward, 1_024_590_163_934);

        // sercomm
        let owner2_reward = *owner_rewards
            .get(&owner2)
            .expect("Could not fetch owner2 rewards");
        assert_eq!(owner2_reward, 1_024_590_163_934);
    }

    /// Test to ensure that rewards that are zeroed are not written out.
    #[tokio::test]
    async fn ensure_zeroed_rewards_are_not_written() {
        use rust_decimal_macros::dec;

        let gw1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed gw1 parse");
        let gw2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed gw2 parse");

        let c1 = "P27-SCE4255W2107CW5000014".to_string();
        let c2 = "P27-SCE4255W2107CW5000015".to_string();
        let c3 = "2AG32PBS3101S1202000464223GY0153".to_string();

        let mut coverage_points = HashMap::new();

        coverage_points.insert(
            gw1.clone(),
            HotspotPoints {
                speedtest_multiplier: dec!(1.0),
                radio_points: vec![(
                    Some(c1),
                    RadioPoints {
                        location_trust_score_multiplier: dec!(1.0),
                        seniority: DateTime::default(),
                        coverage_object: Uuid::new_v4(),
                        points: dec!(10.0),
                        boosted_hexes: vec![],
                    },
                )]
                .into_iter()
                .collect(),
            },
        );
        coverage_points.insert(
            gw2,
            HotspotPoints {
                speedtest_multiplier: dec!(1.0),
                radio_points: vec![
                    (
                        Some(c2),
                        RadioPoints {
                            location_trust_score_multiplier: dec!(1.0),
                            seniority: DateTime::default(),
                            coverage_object: Uuid::new_v4(),
                            points: dec!(-1.0),
                            boosted_hexes: vec![],
                        },
                    ),
                    (
                        Some(c3),
                        RadioPoints {
                            location_trust_score_multiplier: dec!(1.0),
                            points: dec!(0.0),
                            seniority: DateTime::default(),
                            coverage_object: Uuid::new_v4(),
                            boosted_hexes: vec![],
                        },
                    ),
                ]
                .into_iter()
                .collect(),
            },
        );

        let now = Utc::now();
        // We should never see any radio shares from owner2, since all of them are
        // less than or equal to zero.
        let coverage_points = CoveragePoints { coverage_points };
        let epoch = now - Duration::hours(1)..now;
        let total_poc_rewards = get_scheduled_tokens_for_poc(epoch.end - epoch.start);
        let expected_hotspot = gw1;
        for (_reward_amount, mobile_reward) in coverage_points
            .into_rewards(total_poc_rewards, &epoch)
            .unwrap()
        {
            let radio_reward = match mobile_reward.reward {
                Some(proto::mobile_reward_share::Reward::RadioReward(radio_reward)) => radio_reward,
                _ => unreachable!(),
            };
            let actual_hotspot = PublicKeyBinary::from(radio_reward.hotspot_key);
            assert_eq!(actual_hotspot, expected_hotspot);
        }
    }

    #[tokio::test]
    async fn skip_empty_radio_rewards() {
        let coverage_points = CoveragePoints {
            coverage_points: HashMap::new(),
        };

        let now = Utc::now();
        let epoch = now - Duration::hours(1)..now;
        let total_poc_rewards = get_scheduled_tokens_for_poc(epoch.end - epoch.start);
        assert!(coverage_points
            .into_rewards(total_poc_rewards, &epoch)
            .is_none());
    }

    #[tokio::test]
    async fn service_provider_reward_amounts() {
        let mobile_bone_price = dec!(0.00001);

        let sp1 = ServiceProvider::HeliumMobile;

        let now = Utc::now();
        let epoch = (now - Duration::hours(1))..now;

        let service_provider_sessions = vec![ServiceProviderDataSession {
            service_provider: sp1,
            total_dcs: dec!(1000),
        }];
        let sp_shares = ServiceProviderShares::new(service_provider_sessions);
        let total_sp_rewards = get_scheduled_tokens_for_service_providers(epoch.end - epoch.start);
        let rewards_per_share = sp_shares
            .rewards_per_share(total_sp_rewards, mobile_bone_price)
            .unwrap();

        let mut sp_rewards = HashMap::<i32, u64>::new();
        let mut allocated_sp_rewards = 0_u64;
        for (reward_amount, sp_reward) in
            sp_shares.into_service_provider_rewards(&epoch, rewards_per_share)
        {
            if let Some(MobileReward::ServiceProviderReward(r)) = sp_reward.reward {
                sp_rewards.insert(r.service_provider_id, r.amount);
                assert_eq!(reward_amount, r.amount);
                allocated_sp_rewards += reward_amount;
            }
        }

        let sp1_reward_amount = *sp_rewards
            .get(&(sp1 as i32))
            .expect("Could not fetch sp1 shares");
        assert_eq!(sp1_reward_amount, 1000);

        // confirm the unallocated service provider reward amounts
        let unallocated_sp_reward_amount = (total_sp_rewards - Decimal::from(allocated_sp_rewards))
            .round_dp_with_strategy(0, RoundingStrategy::ToZero)
            .to_u64()
            .unwrap_or(0);
        assert_eq!(unallocated_sp_reward_amount, 341_530_053_644);
    }

    #[tokio::test]
    async fn service_provider_reward_amounts_capped() {
        let mobile_bone_price = dec!(1.0);
        let sp1 = ServiceProvider::HeliumMobile;

        let now = Utc::now();
        let epoch = (now - Duration::hours(1))..now;

        let total_sp_rewards_in_bones = dec!(100_000_000);
        let total_rewards_value_in_dc =
            mobile_bones_to_dc(total_sp_rewards_in_bones, mobile_bone_price);

        let service_provider_sessions = vec![ServiceProviderDataSession {
            service_provider: ServiceProvider::HeliumMobile,
            // force the service provider to have spend more DC than total rewardable
            total_dcs: total_rewards_value_in_dc * dec!(2.0),
        }];

        let sp_shares = ServiceProviderShares::new(service_provider_sessions);
        let rewards_per_share = sp_shares
            .rewards_per_share(total_sp_rewards_in_bones, mobile_bone_price)
            .unwrap();

        let mut sp_rewards = HashMap::new();
        let mut allocated_sp_rewards = 0_u64;
        for (reward_amount, sp_reward) in
            sp_shares.into_service_provider_rewards(&epoch, rewards_per_share)
        {
            if let Some(MobileReward::ServiceProviderReward(r)) = sp_reward.reward {
                sp_rewards.insert(r.service_provider_id, r.amount);
                assert_eq!(reward_amount, r.amount);
                allocated_sp_rewards += reward_amount;
            }
        }
        let sp1_reward_amount = *sp_rewards
            .get(&(sp1 as i32))
            .expect("Could not fetch sp1 shares");

        assert_eq!(Decimal::from(sp1_reward_amount), total_sp_rewards_in_bones);
        assert_eq!(sp1_reward_amount, 100_000_000);

        // confirm the unallocated service provider reward amounts
        let unallocated_sp_reward_amount = (total_sp_rewards_in_bones
            - Decimal::from(allocated_sp_rewards))
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0);
        assert_eq!(unallocated_sp_reward_amount, 0);
    }

    #[tokio::test]
    async fn service_provider_reward_hip87_ex1() {
        // mobile price from hip example and converted to bones
        let mobile_bone_price = dec!(0.0001) / dec!(1_000_000);
        let sp1 = ServiceProvider::HeliumMobile;

        let now = Utc::now();
        let epoch = (now - Duration::hours(1))..now;
        let total_sp_rewards_in_bones = dec!(500_000_000) * dec!(1_000_000);

        let service_provider_sessions = vec![ServiceProviderDataSession {
            service_provider: sp1,
            total_dcs: dec!(100_000_000),
        }];

        let sp_shares = ServiceProviderShares::new(service_provider_sessions);
        let rewards_per_share = sp_shares
            .rewards_per_share(total_sp_rewards_in_bones, mobile_bone_price)
            .unwrap();

        let mut sp_rewards = HashMap::new();
        let mut allocated_sp_rewards = 0_u64;
        for (reward_amount, sp_reward) in
            sp_shares.into_service_provider_rewards(&epoch, rewards_per_share)
        {
            if let Some(MobileReward::ServiceProviderReward(r)) = sp_reward.reward {
                sp_rewards.insert(r.service_provider_id, r.amount);
                assert_eq!(reward_amount, r.amount);
                allocated_sp_rewards += reward_amount;
            }
        }

        let sp1_reward_amount_in_bones = *sp_rewards
            .get(&(sp1 as i32))
            .expect("Could not fetch sp1 shares");
        // example in HIP gives expected reward amount in mobile whereas we use bones
        // assert expected value in bones
        assert_eq!(sp1_reward_amount_in_bones, 10_000_000 * 1_000_000);

        // confirm the unallocated service provider reward amounts
        let unallocated_sp_reward_amount = (total_sp_rewards_in_bones
            - Decimal::from(allocated_sp_rewards))
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0);
        assert_eq!(unallocated_sp_reward_amount, 490_000_000_000_000);
    }

    #[tokio::test]
    async fn service_provider_reward_hip87_ex2() {
        // mobile price from hip example and converted to bones
        let mobile_bone_price = dec!(0.0001) / dec!(1_000_000);
        let sp1 = ServiceProvider::HeliumMobile;

        let now = Utc::now();
        let epoch = (now - Duration::hours(24))..now;
        let total_sp_rewards_in_bones = dec!(500_000_000) * dec!(1_000_000);

        let service_provider_sessions = vec![ServiceProviderDataSession {
            service_provider: sp1,
            total_dcs: dec!(100_000_000_000),
        }];

        let sp_shares = ServiceProviderShares::new(service_provider_sessions);
        let rewards_per_share = sp_shares
            .rewards_per_share(total_sp_rewards_in_bones, mobile_bone_price)
            .unwrap();

        let mut sp_rewards = HashMap::new();
        let mut allocated_sp_rewards = 0_u64;
        for (reward_amount, sp_reward) in
            sp_shares.into_service_provider_rewards(&epoch, rewards_per_share)
        {
            if let Some(MobileReward::ServiceProviderReward(r)) = sp_reward.reward {
                sp_rewards.insert(r.service_provider_id, r.amount);
                assert_eq!(reward_amount, r.amount);
                allocated_sp_rewards += reward_amount;
            }
        }

        let sp1_reward_amount_in_bones = *sp_rewards
            .get(&(sp1 as i32))
            .expect("Could not fetch sp1 shares");
        // example in HIP gives expected reward amount in mobile whereas we use bones
        // assert expected value in bones
        assert_eq!(sp1_reward_amount_in_bones, 500_000_000 * 1_000_000);

        // confirm the unallocated service provider reward amounts
        let unallocated_sp_reward_amount = (total_sp_rewards_in_bones
            - Decimal::from(allocated_sp_rewards))
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0);
        assert_eq!(unallocated_sp_reward_amount, 0);
    }
}
