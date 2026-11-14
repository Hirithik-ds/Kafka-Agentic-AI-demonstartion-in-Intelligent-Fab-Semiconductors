"""Static fab master data: tools, process parameters, SPC limits and the process route.

In a real fab this comes from the MES / equipment engineering system. Each
parameter has a target and a sigma derived during tool qualification; the SPC
control limits are target +/- 3 sigma.
"""


def _p(target, sigma, unit):
    return {"target": target, "sigma": sigma, "unit": unit}


_ETCH_PARAMS = {
    "chamber_temp_c": _p(60.0, 0.8, "C"),
    "pressure_mtorr": _p(10.0, 0.25, "mTorr"),
    "rf_power_w": _p(1200.0, 12.0, "W"),
    "he_backside_torr": _p(8.0, 0.15, "Torr"),
}

_CVD_PARAMS = {
    "susceptor_temp_c": _p(400.0, 1.5, "C"),
    "pressure_torr": _p(2.5, 0.03, "Torr"),
    "rf_power_w": _p(500.0, 6.0, "W"),
    "teos_flow_sccm": _p(150.0, 2.0, "sccm"),
}

TOOLS = {
    "LITHO-01": {
        "tool_type": "LITHOGRAPHY", "step": "LITHO", "recipe": "M1_ARF_IMMERSION",
        "params": {
            "stage_temp_c": _p(22.0, 0.02, "C"),
            "dose_mj_cm2": _p(30.0, 0.30, "mJ/cm2"),
            "focus_offset_nm": _p(0.0, 4.0, "nm"),
            "overlay_error_nm": _p(2.0, 0.40, "nm"),
        },
    },
    "ETCH-01": {"tool_type": "PLASMA_ETCH", "step": "ETCH", "recipe": "POLY_GATE_ETCH", "params": _ETCH_PARAMS},
    "ETCH-02": {"tool_type": "PLASMA_ETCH", "step": "ETCH", "recipe": "POLY_GATE_ETCH", "params": _ETCH_PARAMS},
    "CVD-01": {"tool_type": "PECVD", "step": "CVD", "recipe": "TEOS_OXIDE_DEP", "params": _CVD_PARAMS},
    "CVD-02": {"tool_type": "PECVD", "step": "CVD", "recipe": "TEOS_OXIDE_DEP", "params": _CVD_PARAMS},
    "CMP-01": {
        "tool_type": "CMP", "step": "CMP", "recipe": "OXIDE_CMP",
        "params": {
            "platen_speed_rpm": _p(93.0, 0.8, "rpm"),
            "down_force_psi": _p(3.0, 0.05, "psi"),
            "slurry_flow_ml_min": _p(200.0, 3.0, "ml/min"),
            "pad_temp_c": _p(45.0, 0.6, "C"),
        },
    },
}

# Simplified front-end-of-line route every wafer lot follows.
ROUTE = ["LITHO", "ETCH", "CVD", "CMP", "METROLOGY"]
METROLOGY_TOOL = "METRO-01"
PRODUCT = "SOC-N7-A1"
WAFERS_PER_LOT = 25


def tools_for_step(step: str) -> list:
    return [tool_id for tool_id, spec in TOOLS.items() if spec["step"] == step]


def param_spec(tool_id: str, param: str) -> dict:
    return TOOLS[tool_id]["params"][param]


def z_score(tool_id: str, param: str, value: float) -> float:
    spec = param_spec(tool_id, param)
    return (value - spec["target"]) / spec["sigma"]


def control_limits(tool_id: str, param: str):
    spec = param_spec(tool_id, param)
    return spec["target"] - 3 * spec["sigma"], spec["target"] + 3 * spec["sigma"]
