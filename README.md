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
cd ..
```
