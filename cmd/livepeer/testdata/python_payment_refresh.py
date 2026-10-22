"""Exercise real SDK signed-state refresh across nonce, block and auth expiry."""

import asyncio
import base64
import json
import sys
from dataclasses import replace

import aiohttp
from livepeer_gateway.remote_signer import LivePaymentChallenge, LivePaymentSession, get_signer_info


async def main(orchestrator: str, signer: str) -> None:
    material = await get_signer_info(signer)
    async with aiohttp.ClientSession() as http:
        async with http.post(
            f"{orchestrator}/apps/live/session",
            headers={"Livepeer-Payer-Address": material.address},
        ) as response:
            assert response.status == 402, await response.text()
            challenge = await response.json()
        session = LivePaymentSession(
            signer,
            type="live",
            app="refresh-test",
            challenge=LivePaymentChallenge(
                payment_params=challenge["payment_params"],
                manifest_id=challenge["manifest_id"],
                payment_url=challenge["payment_url"],
            ),
        )
        payment = await session.get_payment()
        async with http.post(
            f"{orchestrator}/apps/live/session",
            headers={"Livepeer-Payment": payment.payment, "Livepeer-Segment": payment.seg_creds},
        ) as response:
            assert response.status == 200, await response.text()
            reservation = await response.json()

        def state():
            return json.loads(base64.b64decode(session._state["state"]))

        first = state()
        assert first["SenderNonce"] == 100, first
        # Accelerated payment cycles exercise the same opaque-state path as
        # run_payments, without waiting minutes for its ordinary 3s interval.
        for _ in range(45):
            await session.send_payment()
        rolled = state()
        assert rolled["PMSessionID"] != first["PMSessionID"], rolled
        assert rolled["SequenceNumber"] == 45, rolled

        async with http.post(f"{orchestrator}/test/advance") as response:
            assert response.status == 200
        await session.send_payment()
        expired = state()
        assert expired["PMSessionID"] != rolled["PMSessionID"], expired
        assert expired["SequenceNumber"] == 46

        async with http.post(
            f"{orchestrator}/test/expire-auth",
            data=base64.b64decode(session._challenge.payment_params),
        ) as response:
            assert response.status == 200, await response.text()
            session._challenge = replace(
                session._challenge,
                payment_params=base64.b64encode(await response.read()).decode(),
            )
        await session.send_payment()
        renewed = state()
        assert renewed["PMSessionID"] != expired["PMSessionID"], renewed
        assert renewed["SequenceNumber"] == 47
        assert renewed["ManifestID"] == reservation["session_id"]
        async with http.post(reservation["control_url"] + "/stop") as response:
            assert response.status == 200
    print("Python payment nonce, L1 expiry and auth refresh passed with signed state preserved")


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
