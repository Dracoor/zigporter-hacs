"""Tests for device_lqi.py — tracking the LQI Zigbee2MQTT reports per device."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.zigporter.device_lqi import (
    Z2MDeviceLqiTracker,
    _read_text,
    _valid_lqi,
    _write_text,
)


@pytest.fixture
def hass():
    hass = MagicMock()

    async def run(func, *args):
        return func(*args)

    hass.async_add_executor_job = AsyncMock(side_effect=run)
    return hass


def _msg(topic, payload):
    msg = MagicMock()
    msg.topic = topic
    msg.payload = payload
    return msg


class TestOnMessage:
    def test_records_linkquality(self, hass):
        tracker = Z2MDeviceLqiTracker(hass, "zigbee2mqtt", None)
        tracker._on_message(_msg("zigbee2mqtt/Front Door", '{"contact":true,"linkquality":111}'))
        assert tracker.lqi_by_name() == {"Front Door": 111}
        assert "time" in tracker.values["Front Door"]

    def test_accepts_bytes_payload(self, hass):
        tracker = Z2MDeviceLqiTracker(hass, "zigbee2mqtt", None)
        tracker._on_message(_msg("zigbee2mqtt/Door", b'{"linkquality":60}'))
        assert tracker.lqi_by_name() == {"Door": 60}

    def test_nested_friendly_name(self, hass):
        tracker = Z2MDeviceLqiTracker(hass, "zigbee2mqtt/", None)
        tracker._on_message(_msg("zigbee2mqtt/garage/door", '{"linkquality":90}'))
        assert tracker.lqi_by_name() == {"garage/door": 90}

    def test_latest_value_wins(self, hass):
        tracker = Z2MDeviceLqiTracker(hass, "zigbee2mqtt", None)
        tracker._on_message(_msg("zigbee2mqtt/Door", '{"linkquality":60}'))
        tracker._on_message(_msg("zigbee2mqtt/Door", '{"linkquality":72}'))
        assert tracker.lqi_by_name() == {"Door": 72}

    @pytest.mark.parametrize(
        ("topic", "payload"),
        [
            ("other/Door", '{"linkquality":60}'),
            ("zigbee2mqtt/", '{"linkquality":60}'),
            ("zigbee2mqtt/bridge", '{"linkquality":60}'),
            ("zigbee2mqtt/bridge/state", '{"linkquality":60}'),
            ("zigbee2mqtt/Door/set", '{"linkquality":60}'),
            ("zigbee2mqtt/Door/get", '{"linkquality":60}'),
            ("zigbee2mqtt/Door/availability", '{"linkquality":60}'),
            ("zigbee2mqtt/Door", '{"contact":true}'),
            ("zigbee2mqtt/Door", 'not json "linkquality"'),
            ("zigbee2mqtt/Door", '["linkquality"]'),
            ("zigbee2mqtt/Door", '{"linkquality":0}'),
            ("zigbee2mqtt/Door", '{"linkquality":"60"}'),
            ("zigbee2mqtt/Door", '{"linkquality":true}'),
            ("zigbee2mqtt/Door", '{"linkquality":300}'),
            ("zigbee2mqtt/Door", b"\xff\xfe"),
            ("zigbee2mqtt/Door", None),
        ],
    )
    def test_ignores_non_device_or_invalid(self, hass, topic, payload):
        tracker = Z2MDeviceLqiTracker(hass, "zigbee2mqtt", None)
        tracker._on_message(_msg(topic, payload))
        assert tracker.values == {}


class TestStartStop:
    async def test_start_subscribes_once(self, hass):
        tracker = Z2MDeviceLqiTracker(hass, "zigbee2mqtt_acreage", None)
        unsub = MagicMock()
        with (
            patch(
                "custom_components.zigporter.device_lqi.mqtt.async_wait_for_mqtt_client",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "custom_components.zigporter.device_lqi.mqtt.async_subscribe",
                new_callable=AsyncMock,
                return_value=unsub,
            ) as subscribe,
        ):
            await tracker.async_start()
            await tracker.async_start()

        subscribe.assert_awaited_once()
        assert subscribe.await_args.args[1] == "zigbee2mqtt_acreage/#"

        tracker.async_stop()
        unsub.assert_called_once()
        tracker.async_stop()
        unsub.assert_called_once()

    async def test_start_without_mqtt(self, hass):
        tracker = Z2MDeviceLqiTracker(hass, "zigbee2mqtt", None)
        with (
            patch(
                "custom_components.zigporter.device_lqi.mqtt.async_wait_for_mqtt_client",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                "custom_components.zigporter.device_lqi.mqtt.async_subscribe",
                new_callable=AsyncMock,
            ) as subscribe,
        ):
            await tracker.async_start()
        subscribe.assert_not_awaited()


class TestPersistence:
    async def test_save_and_load_roundtrip(self, hass, tmp_path):
        path = str(tmp_path / "sub" / "device_lqi.json")
        tracker = Z2MDeviceLqiTracker(hass, "zigbee2mqtt", path)
        tracker._on_message(_msg("zigbee2mqtt/Door", '{"linkquality":60}'))
        await tracker.async_save()

        loaded = Z2MDeviceLqiTracker(hass, "zigbee2mqtt", path)
        await loaded.async_load()
        assert loaded.lqi_by_name() == {"Door": 60}

    async def test_load_drops_invalid_entries(self, hass, tmp_path):
        path = tmp_path / "device_lqi.json"
        path.write_text(json.dumps({"A": {"lqi": 50}, "B": {"lqi": 0}, "C": 7}))
        tracker = Z2MDeviceLqiTracker(hass, "zigbee2mqtt", str(path))
        await tracker.async_load()
        assert tracker.lqi_by_name() == {"A": 50}

    async def test_load_missing_file(self, hass, tmp_path):
        tracker = Z2MDeviceLqiTracker(hass, "zigbee2mqtt", str(tmp_path / "none.json"))
        await tracker.async_load()
        assert tracker.values == {}

    async def test_load_corrupt_file(self, hass, tmp_path):
        path = tmp_path / "device_lqi.json"
        path.write_text("{broken")
        tracker = Z2MDeviceLqiTracker(hass, "zigbee2mqtt", str(path))
        await tracker.async_load()
        assert tracker.values == {}

    async def test_load_non_dict_file(self, hass, tmp_path):
        path = tmp_path / "device_lqi.json"
        path.write_text("[1, 2]")
        tracker = Z2MDeviceLqiTracker(hass, "zigbee2mqtt", str(path))
        await tracker.async_load()
        assert tracker.values == {}

    async def test_load_read_error(self, hass):
        hass.async_add_executor_job = AsyncMock(side_effect=OSError("denied"))
        tracker = Z2MDeviceLqiTracker(hass, "zigbee2mqtt", "/nope/device_lqi.json")
        await tracker.async_load()
        assert tracker.values == {}

    async def test_no_store_path_is_noop(self, hass):
        tracker = Z2MDeviceLqiTracker(hass, "zigbee2mqtt", None)
        tracker._on_message(_msg("zigbee2mqtt/Door", '{"linkquality":60}'))
        await tracker.async_load()
        await tracker.async_save()
        hass.async_add_executor_job.assert_not_awaited()

    async def test_save_skips_when_empty(self, hass, tmp_path):
        tracker = Z2MDeviceLqiTracker(hass, "zigbee2mqtt", str(tmp_path / "x.json"))
        await tracker.async_save()
        hass.async_add_executor_job.assert_not_awaited()

    async def test_save_write_error(self, hass):
        hass.async_add_executor_job = AsyncMock(side_effect=OSError("disk full"))
        tracker = Z2MDeviceLqiTracker(hass, "zigbee2mqtt", "/nope/device_lqi.json")
        tracker._on_message(_msg("zigbee2mqtt/Door", '{"linkquality":60}'))
        await tracker.async_save()  # logs, does not raise


class TestHelpers:
    def test_valid_lqi(self):
        assert _valid_lqi(1)
        assert _valid_lqi(255.0)
        assert not _valid_lqi(0)
        assert not _valid_lqi(256)
        assert not _valid_lqi(True)
        assert not _valid_lqi(None)

    def test_read_write(self, tmp_path):
        path = str(tmp_path / "a" / "b.json")
        assert _read_text(path) == ""
        _write_text(path, "hi")
        assert _read_text(path) == "hi"
