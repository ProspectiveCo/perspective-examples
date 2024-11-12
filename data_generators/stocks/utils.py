"""
Utility module for generation and modifying stock trade sample date.
"""

import numpy as np
import pandas as pd
import math

__all__ = (
    "generate_varying_amplitude_wave"
)


def generate_varying_amplitude_wave(
        length,                     # length of time series generated
        max_amp,                    # maximum amplitude variation in Y axis
        frequency: int = 2, 
        periods: int = 3,
        seed=None) -> pd.Series:
    """
    Generates a smooth sinusoidal wave with uniform frequency and varying amplitudes for each peak and trough.

    Parameters:
    length (int): Number of data points (length of time series).
    frequency (float): Frequency of the sinusoidal wave.
    max_amp (float): Maximum amplitude for the peaks.
    seed (int): Random seed for reproducibility (default is None).

    Returns:
    pd.Series: A pandas Series containing the generated wave.
    """
    # check, correct, apply params
    if seed is not None:
        np.random.seed(seed)

    # Time vector
    time = np.linspace(0, periods * np.pi, length)
    # Generate the base sinusoidal wave with uniform frequency and phase
    wave = np.sin(frequency * time)

    # get indices for the start/mid/end point of each period of the wave
    # Calculate the number of points per period
    points_per_period = len(time) // periods
    # Initialize the list of tuples to store start, mid, and end indices
    period_indices = []
    # Loop over each period and calculate the start, mid, and end indices
    # for i in range(int(frequency)):
    for i in range(periods):
        start = int(i * points_per_period)
        end = ((i + 1) * points_per_period) + 1
        mid = ((start + end) // 2) + 1
        period_indices.append((start, mid, end))

    # attenuate the wave period by period by a random amplitude
    for period in period_indices:
        # print(f"period: {period}")
        start, mid, end = period
        wave[start:mid] *= np.random.randint(1, abs(max_amp))
        wave[mid:end] *= np.random.randint(1, abs(max_amp))
        # Modify wave values by multiplying element by element
        # period_length = end - start + 1
        # ts = np.linspace(0, 2 * np.pi, period_length) * high
        # wave2[start:(end +1)] *= ts

    # return the wave
    return wave


def shift_wave(wave: pd.Series, y: int) -> pd.Series:
    return wave + y
