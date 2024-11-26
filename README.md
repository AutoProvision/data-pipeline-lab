### Docker

Configurando ambiente da VM
```bash
sudo apt install docker-compose
sudo usermod -aG docker $USER
exit
```

Iniciando processo
```bash
sudo rm -rf data-pipeline-lab
git clone https://github.com/AutoProvision/data-pipeline-lab.git
cd data-pipeline-lab
docker-compose build
docker-compose up
```

### Venv

Configurando ambiente da VM
```bash
sudo apt install python3-venv
```

Iniciando processo
```bash
sudo rm -rf data-pipeline-lab
git clone https://github.com/AutoProvision/data-pipeline-lab.git
cd data-pipeline-lab
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python main.py
```
