import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { MapContainer, TileLayer, GeoJSON } from 'react-leaflet';
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import 'leaflet/dist/leaflet.css';

const FloodDashboard = () => {
  const [landUseData, setLandUseData] = useState(null);
  const [socioData, setSocioData] = useState([]);
  const [sentinelData, setSentinelData] = useState([]);
  const [weatherData, setWeatherData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        // Fetch land use GeoJSON for map
        try {
          const landUseRes = await axios.get('http://localhost:5000/api/landuse/lagos');
          setLandUseData(landUseRes.data);
        } catch (err) {
          console.error('Land Use fetch error:', err.message);
          throw new Error('Failed to load land use data');
        }
        
        // Fetch socioeconomic features
        try {
          const socioRes = await axios.get('http://localhost:5000/api/socioeconomic');
          console.log('Socioeconomic Data:', socioRes.data.slice(0, 5));
          setSocioData(socioRes.data);
        } catch (err) {
          console.error('Socioeconomic fetch error:', err.message);
          throw new Error('Failed to load socioeconomic data');
        }
        
        // Fetch sentinel features
        try {
          const sentinelRes = await axios.get('http://localhost:5000/api/sentinel_features');
          console.log('Sentinel Data:', sentinelRes.data);
          setSentinelData(sentinelRes.data);
        } catch (err) {
          console.error('Sentinel fetch error:', err.message);
          throw new Error('Failed to load Sentinel data');
        }
        
        // Fetch weather features
        try {
          const weatherRes = await axios.get('http://localhost:5000/api/weather_features');
          console.log('Weather Data:', weatherRes.data.slice(0, 5));
          setWeatherData(weatherRes.data);
        } catch (err) {
          console.error('Weather fetch error:', err.message);
          throw new Error('Failed to load weather data');
        }
      } catch (err) {
        console.error('Fetch error:', err);
        setError(err.message || 'Failed to load data. Please check the backend server.');
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, []);

  if (loading) return <div className="text-center text-xl">Loading...</div>;
  if (error) return <div className="text-center text-xl text-red-500">{error}</div>;

  const landUseColors = {
    residential: '#ff9999',
    farmland: '#99cc99',
    forest: '#339933',
    grass: '#ccff99',
    industrial: '#9999cc',
    cemetery: '#666666',
    commercial: '#ffcc00'
  };

  const socioChartData = socioData
    .filter(item => item.state && item.state.toLowerCase() === 'lagos' && item.area_sqm)
    .map(item => ({ landuse: item.landuse_type, area_sqm: item.area_sqm }));

  const sentinelChartData = sentinelData
    .filter(item => item.region && item.region.toLowerCase() === 'lagos')
    .map(item => ({ week: item.week_start_date, count: item.image_count }));

  const weatherChartData = weatherData
    .filter(item => item.city && item.city.toLowerCase() === 'lagos' && item.avg_precipitation_7d != null)
    .map(item => ({ week: item.window_start_date, precipitation: item.avg_precipitation_7d }));

  return (
    <div className="container mx-auto p-4 bg-white rounded-lg shadow-lg">
      <h1 className="text-3xl font-bold mb-4 text-center">Flood Prediction Dashboard</h1>
      
      <section className="mb-6">
        <h2 className="text-2xl font-semibold mb-2">Lagos Land Use Map</h2>
        <MapContainer center={[6.5, 3.3]} zoom={10} style={{ height: '500px' }} className="rounded-lg shadow-md">
          <TileLayer
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            attribution='© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          />
          {landUseData && (
            <GeoJSON
              data={landUseData}
              style={feature => ({
                fillColor: landUseColors[feature.properties.landuse] || '#cccccc',
                weight: 1,
                color: '#000000',
                fillOpacity: 0.6
              })}
              onEachFeature={(feature, layer) => {
                layer.bindPopup(`Land Use: ${feature.properties.landuse || 'Unknown'}<br>ID: ${feature.properties.id || 'N/A'}`);
              }}
            />
          )}
        </MapContainer>
      </section>
      
      <section className="mb-6">
        <h2 className="text-2xl font-semibold mb-2">Land Use Area (Lagos)</h2>
        {socioChartData.length > 0 ? (
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={socioChartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="landuse" fontSize={12} />
              <YAxis fontSize={12} />
              <Tooltip />
              <Legend />
              <Bar dataKey="area_sqm" fill="#8884d8" name="Area (sqm)" />
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <div className="text-center text-gray-500">No land use data available for Lagos</div>
        )}
      </section>
      
      <section className="mb-6">
        <h2 className="text-2xl font-semibold mb-2">Sentinel Imagery (Lagos)</h2>
        {sentinelChartData.length > 0 ? (
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={sentinelChartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="week" fontSize={12} />
              <YAxis fontSize={12} />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="count" stroke="#82ca9d" name="Image Count" />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <div className="text-center text-gray-500">No Sentinel data available for Lagos</div>
        )}
      </section>
      
      <section className="mb-6">
        <h2 className="text-2xl font-semibold mb-2">Average Precipitation (Lagos)</h2>
        {weatherChartData.length > 0 ? (
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={weatherChartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="week" fontSize={12} />
              <YAxis fontSize={12} />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="precipitation" stroke="#ff7300" name="Avg Precipitation (7d)" />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <div className="text-center text-gray-500">No precipitation data available for Lagos</div>
        )}
      </section>
    </div>
  );
};

export default FloodDashboard;
